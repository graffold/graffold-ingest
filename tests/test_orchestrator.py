"""Tests for the full orchestrator pipeline (mocked externals)."""

import json
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from graffold_ingest.connectors.base import Document, ExtractionResult


LLM_RESPONSE = json.dumps({
    "nodes": [
        {"id": "tp53", "label": "Protein", "name": "TP53", "properties": {}},
    ],
    "edges": [],
})


class TestOrchestratorFlow:
    """Test the orchestrator wires stages together correctly."""

    @pytest.mark.asyncio
    async def test_pipeline_stages_execute_in_order(self, tmp_path):
        from graffold_ingest.pipeline.chunk import chunk_documents

        docs = [Document(id="d1", content="TP53 is a protein.", source_type="web")]
        chunks = chunk_documents(docs)
        assert len(chunks) == 1

        with patch(
            "graffold_ingest.pipeline.extract._call_llm",
            new_callable=AsyncMock,
            return_value=LLM_RESPONSE,
        ):
            from graffold_ingest.pipeline.extract import extract_entities

            results = await extract_entities(chunks)
            assert len(results) == 1
            assert results[0].nodes[0]["name"] == "TP53"

        from graffold_ingest.pipeline.publish_parquet import publish_to_parquet

        counts = await publish_to_parquet(results, output_dir=tmp_path)
        assert counts["entities_written"] == 1

    @pytest.mark.asyncio
    async def test_pipeline_survives_extraction_failure(self, tmp_path):
        """Pipeline should produce empty results, not crash."""
        docs = [Document(id="d1", content="broken content", source_type="web")]

        with patch(
            "graffold_ingest.pipeline.extract._call_llm",
            new_callable=AsyncMock,
            side_effect=RuntimeError("LLM timeout"),
        ):
            from graffold_ingest.pipeline.extract import extract_entities

            results = await extract_entities(docs)
            # Should get empty result, not exception
            assert len(results) == 1
            assert results[0].nodes == []

        from graffold_ingest.pipeline.publish_parquet import publish_to_parquet

        counts = await publish_to_parquet(results, output_dir=tmp_path)
        assert counts["entities_written"] == 0

    @pytest.mark.asyncio
    async def test_chunk_extract_resolve_publish_roundtrip(self, tmp_path):
        """Full pipeline roundtrip: chunk → extract → resolve → publish → read."""
        from graffold_ingest.pipeline.chunk import chunk_documents
        from graffold_ingest.pipeline.extract import extract_entities
        from graffold_ingest.pipeline.resolve import resolve_entities
        from graffold_ingest.pipeline.publish_parquet import (
            publish_to_parquet,
            read_parquet_graph,
        )

        docs = [Document(id="d1", content="BRCA1 interacts with TP53.", source_type="pdf")]
        chunks = chunk_documents(docs)

        multi_node_response = json.dumps({
            "nodes": [
                {"id": "brca1", "label": "Protein", "name": "BRCA1", "properties": {}},
                {"id": "tp53", "label": "Protein", "name": "TP53", "properties": {}},
            ],
            "edges": [
                {"source": "brca1", "target": "tp53", "type": "INTERACTS_WITH", "properties": {}}
            ],
        })

        with patch(
            "graffold_ingest.pipeline.extract._call_llm",
            new_callable=AsyncMock,
            return_value=multi_node_response,
        ):
            results = await extract_entities(chunks)

        resolved = resolve_entities(results)
        assert len(resolved) == 1

        await publish_to_parquet(resolved, output_dir=tmp_path)
        entities, relationships = read_parquet_graph(tmp_path)
        assert len(entities) == 2
        assert len(relationships) == 1
