"""Tests for Parquet publish backend and dual-write mode."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from graffold_ingest.connectors.base import ExtractionResult
from graffold_ingest.pipeline.dual_write import publish_dual
from graffold_ingest.pipeline.publish_parquet import (
    parquet_stats,
    publish_to_parquet,
    read_parquet_graph,
)

# ─── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_results() -> list[ExtractionResult]:
    """Create sample extraction results for testing."""
    return [
        ExtractionResult(
            source_doc_id="doc-001",
            nodes=[
                {"id": "n1", "name": "BRCA1", "type": "Gene", "description": "Tumor suppressor"},
                {"id": "n2", "name": "TP53", "type": "Gene", "description": "Guardian of genome"},
            ],
            edges=[
                {
                    "source_id": "n1",
                    "target_id": "n2",
                    "type": "INTERACTS_WITH",
                    "weight": 0.9,
                    "description": "DNA repair pathway",
                },
            ],
        ),
    ]


@pytest.fixture
def second_results() -> list[ExtractionResult]:
    """Additional results for append/merge testing."""
    return [
        ExtractionResult(
            source_doc_id="doc-002",
            nodes=[
                {"id": "n2", "name": "TP53-updated", "type": "Gene", "description": "Updated"},
                {"id": "n3", "name": "MDM2", "type": "Gene", "description": "TP53 regulator"},
            ],
            edges=[
                {
                    "source_id": "n2",
                    "target_id": "n3",
                    "type": "REGULATES",
                    "weight": 0.8,
                    "description": "Negative regulation",
                },
            ],
        ),
    ]


# ─── publish_to_parquet tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_creates_files(tmp_path, sample_results):
    """Parquet files are created in output directory."""
    counts = await publish_to_parquet(sample_results, output_dir=tmp_path)

    assert (tmp_path / "entities.parquet").exists()
    assert (tmp_path / "relationships.parquet").exists()
    assert counts["entities_written"] == 2
    assert counts["relationships_written"] == 1


@pytest.mark.asyncio
async def test_entities_deduplicated_by_id(tmp_path, sample_results, second_results):
    """With latest=True, entities with same ID collapse to newest (MERGE-on-read)."""
    await publish_to_parquet(sample_results, output_dir=tmp_path)
    await publish_to_parquet(second_results, output_dir=tmp_path)

    nodes, _ = read_parquet_graph(output_dir=tmp_path, latest=True)
    ids = [n["id"] for n in nodes]

    # n2 collapses to one row (latest wins)
    assert ids.count("n2") == 1
    # Total unique entities: n1, n2, n3
    assert len(nodes) == 3

    # n2 should have the updated name from second write
    n2 = next(n for n in nodes if n["id"] == "n2")
    assert n2["name"] == "TP53-updated"


@pytest.mark.asyncio
async def test_append_only_keeps_all_versions(tmp_path, sample_results, second_results):
    """Raw read (latest=False) keeps every version — append-only provenance log."""
    await publish_to_parquet(sample_results, output_dir=tmp_path)
    await publish_to_parquet(second_results, output_dir=tmp_path)

    nodes, _ = read_parquet_graph(output_dir=tmp_path)  # raw
    ids = [n["id"] for n in nodes]

    # n2 written twice → appears twice in the raw append log
    assert ids.count("n2") == 2


@pytest.mark.asyncio
async def test_append_mode(tmp_path, sample_results, second_results):
    """Two writes collapse to all unique entities with latest=True."""
    await publish_to_parquet(sample_results, output_dir=tmp_path)
    await publish_to_parquet(second_results, output_dir=tmp_path)

    nodes, edges = read_parquet_graph(output_dir=tmp_path, latest=True)

    assert len(nodes) == 3  # n1, n2 (deduped), n3
    assert len(edges) == 2  # relationships collapsed by (source, target, type)


@pytest.mark.asyncio
async def test_read_parquet_graph(tmp_path, sample_results):
    """read_parquet_graph returns correct data."""
    await publish_to_parquet(sample_results, output_dir=tmp_path)

    nodes, edges = read_parquet_graph(output_dir=tmp_path)

    assert len(nodes) == 2
    assert len(edges) == 1
    assert nodes[0]["id"] == "n1"
    assert nodes[0]["name"] == "BRCA1"
    assert edges[0]["source_id"] == "n1"
    assert edges[0]["target_id"] == "n2"
    assert edges[0]["type"] == "INTERACTS_WITH"


@pytest.mark.asyncio
async def test_read_parquet_graph_empty_dir(tmp_path):
    """read_parquet_graph returns empty lists for nonexistent dir."""
    nodes, edges = read_parquet_graph(output_dir=tmp_path)
    assert nodes == []
    assert edges == []


@pytest.mark.asyncio
async def test_parquet_stats(tmp_path, sample_results):
    """parquet_stats counts correctly."""
    await publish_to_parquet(
        sample_results,
        output_dir=tmp_path,
        communities=[
            {"id": "c1", "level": 0, "title": "Cluster A", "summary": "...", "size": 2},
        ],
    )

    stats = parquet_stats(output_dir=tmp_path)

    assert stats["entities"] == 2
    assert stats["relationships"] == 1
    assert stats["communities"] == 1
    assert stats["text_units"] == 0
    assert stats["documents"] == 0


@pytest.mark.asyncio
async def test_publish_with_documents_and_text_units(tmp_path, sample_results):
    """Documents and text_units are written when provided."""
    docs = [{"id": "doc-001", "title": "Paper A", "source_url": "https://example.com", "source_type": "web"}]
    text_units = [
        {
            "id": "tu-001",
            "text": "Some chunk text",
            "source_doc_id": "doc-001",
            "entity_ids": ["n1", "n2"],
            "relationship_ids": ["n1-n2"],
        },
    ]

    counts = await publish_to_parquet(
        sample_results,
        output_dir=tmp_path,
        documents=docs,
        text_units=text_units,
    )

    assert counts["documents_written"] == 1
    assert counts["text_units_written"] == 1

    stats = parquet_stats(output_dir=tmp_path)
    assert stats["documents"] == 1
    assert stats["text_units"] == 1


@pytest.mark.asyncio
async def test_publish_communities(tmp_path, sample_results):
    """Community records are written and deduplicated."""
    communities = [
        {"community_id": "c1", "level": 0, "title": "DNA Repair", "summary": "Genes in DNA repair", "member_count": 2},
        {"community_id": "c2", "level": 1, "title": "Cancer", "summary": "Cancer genes", "parent_id": "c1", "member_count": 5},
    ]

    counts = await publish_to_parquet(
        sample_results,
        output_dir=tmp_path,
        communities=communities,
    )

    assert counts["communities_written"] == 2
    assert parquet_stats(output_dir=tmp_path)["communities"] == 2


# ─── dual_write tests ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dual_write_both_enabled(tmp_path, sample_results):
    """Dual write publishes to both backends."""
    with patch(
        "graffold_ingest.pipeline.dual_write.publish_to_graph",
        new_callable=AsyncMock,
        return_value={"nodes_created": 2, "edges_created": 1},
    ) as mock_neo4j:
        result = await publish_dual(
            sample_results,
            neo4j_enabled=True,
            parquet_enabled=True,
            parquet_dir=tmp_path,
        )

    mock_neo4j.assert_called_once()
    assert result["neo4j"] == {"nodes_created": 2, "edges_created": 1}
    assert result["parquet"]["entities_written"] == 2
    assert result["parquet"]["relationships_written"] == 1
    assert result["errors"] == []


@pytest.mark.asyncio
async def test_dual_write_neo4j_disabled(tmp_path, sample_results):
    """When neo4j_enabled=False, only Parquet is written."""
    with patch(
        "graffold_ingest.pipeline.dual_write.publish_to_graph",
        new_callable=AsyncMock,
    ) as mock_neo4j:
        result = await publish_dual(
            sample_results,
            neo4j_enabled=False,
            parquet_enabled=True,
            parquet_dir=tmp_path,
        )

    mock_neo4j.assert_not_called()
    assert result["neo4j"] == {}
    assert result["parquet"]["entities_written"] == 2


@pytest.mark.asyncio
async def test_dual_write_parquet_disabled(tmp_path, sample_results):
    """When parquet_enabled=False, only Neo4j is written."""
    with patch(
        "graffold_ingest.pipeline.dual_write.publish_to_graph",
        new_callable=AsyncMock,
        return_value={"nodes_created": 2, "edges_created": 1},
    ) as mock_neo4j:
        result = await publish_dual(
            sample_results,
            neo4j_enabled=True,
            parquet_enabled=False,
            parquet_dir=tmp_path,
        )

    mock_neo4j.assert_called_once()
    assert result["neo4j"] == {"nodes_created": 2, "edges_created": 1}
    assert result["parquet"] == {}


@pytest.mark.asyncio
async def test_dual_write_survives_neo4j_failure(tmp_path, sample_results):
    """Parquet write succeeds even if Neo4j fails."""
    with patch(
        "graffold_ingest.pipeline.dual_write.publish_to_graph",
        new_callable=AsyncMock,
        side_effect=ConnectionError("Neo4j unavailable"),
    ):
        result = await publish_dual(
            sample_results,
            neo4j_enabled=True,
            parquet_enabled=True,
            parquet_dir=tmp_path,
        )

    assert result["neo4j"] == {}
    assert result["parquet"]["entities_written"] == 2
    assert len(result["errors"]) == 1
    assert result["errors"][0]["backend"] == "neo4j"


@pytest.mark.asyncio
async def test_dual_write_survives_parquet_failure(tmp_path, sample_results):
    """Neo4j write succeeds even if Parquet fails."""
    with (
        patch(
            "graffold_ingest.pipeline.dual_write.publish_to_graph",
            new_callable=AsyncMock,
            return_value={"nodes_created": 2, "edges_created": 1},
        ),
        patch(
            "graffold_ingest.pipeline.dual_write.publish_to_parquet",
            new_callable=AsyncMock,
            side_effect=OSError("Disk full"),
        ),
    ):
        result = await publish_dual(
            sample_results,
            neo4j_enabled=True,
            parquet_enabled=True,
            parquet_dir=tmp_path,
        )

    assert result["neo4j"] == {"nodes_created": 2, "edges_created": 1}
    assert result["parquet"] == {}
    assert len(result["errors"]) == 1
    assert result["errors"][0]["backend"] == "parquet"
