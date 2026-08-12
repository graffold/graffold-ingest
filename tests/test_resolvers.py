"""Tests for the resolvers package."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from graffold_ingest.connectors.base import ExtractionResult
from graffold_ingest.resolvers import (
    CompositeResolver,
    MONDOResolver,
    PubChemResolver,
    UniProtResolver,
    resolve_entities_enhanced,
)
from graffold_ingest.resolvers.base import ResolvedEntity

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_caches():
    """Clear resolver caches between tests."""
    from graffold_ingest.resolvers import mondo, pubchem, uniprot

    uniprot._cache.clear()
    mondo._cache.clear()
    pubchem._cache.clear()
    yield
    uniprot._cache.clear()
    mondo._cache.clear()
    pubchem._cache.clear()


def _mock_response(json_data: dict, status_code: int = 200) -> httpx.Response:
    """Build a mock httpx.Response."""
    return httpx.Response(
        status_code=status_code,
        json=json_data,
        request=httpx.Request("GET", "https://example.com"),
    )


# ---------------------------------------------------------------------------
# UniProt resolver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uniprot_resolver_success():
    """UniProt resolver returns canonical protein info."""
    resolver = UniProtResolver()
    mock_json = {
        "results": [
            {
                "primaryAccession": "P04637",
                "proteinDescription": {
                    "recommendedName": {
                        "fullName": {"value": "Cellular tumor antigen p53"}
                    }
                },
                "organism": {"scientificName": "Homo sapiens"},
            }
        ]
    }

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = _mock_response(mock_json)
        result = await resolver.resolve("TP53", "Protein")

    assert result is not None
    assert result.canonical_id == "P04637"
    assert result.canonical_name == "Cellular tumor antigen p53"
    assert result.resolver == "uniprot"
    assert result.metadata["organism"] == "Homo sapiens"
    assert result.confidence == 0.9


@pytest.mark.asyncio
async def test_uniprot_resolver_no_results():
    """UniProt resolver returns None when no results found."""
    resolver = UniProtResolver()
    mock_json = {"results": []}

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = _mock_response(mock_json)
        result = await resolver.resolve("NotARealProtein123", "Protein")

    assert result is None


@pytest.mark.asyncio
async def test_uniprot_resolver_handles():
    """UniProt resolver only handles Protein/Target/Enzyme labels."""
    resolver = UniProtResolver()
    assert resolver.handles("Protein") is True
    assert resolver.handles("Target") is True
    assert resolver.handles("Enzyme") is True
    assert resolver.handles("Disease") is False
    assert resolver.handles("Compound") is False


# ---------------------------------------------------------------------------
# MONDO resolver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mondo_resolver_success():
    """MONDO resolver returns canonical disease ID."""
    resolver = MONDOResolver()
    mock_json = {
        "response": {
            "docs": [
                {
                    "obo_id": "MONDO:0005149",
                    "label": "diabetes mellitus (disease)",
                    "synonym": ["diabetes", "DM"],
                }
            ]
        }
    }

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = _mock_response(mock_json)
        result = await resolver.resolve("diabetes", "Disease")

    assert result is not None
    assert result.canonical_id == "MONDO:0005149"
    assert result.canonical_name == "diabetes mellitus (disease)"
    assert result.resolver == "mondo"
    assert "diabetes" in result.metadata["synonyms"]


@pytest.mark.asyncio
async def test_mondo_resolver_handles():
    """MONDO resolver only handles Disease/Condition/Disorder labels."""
    resolver = MONDOResolver()
    assert resolver.handles("Disease") is True
    assert resolver.handles("Condition") is True
    assert resolver.handles("Disorder") is True
    assert resolver.handles("Protein") is False


# ---------------------------------------------------------------------------
# PubChem resolver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pubchem_resolver_success():
    """PubChem resolver returns CID and IUPAC name."""
    resolver = PubChemResolver()
    mock_json = {
        "PropertyTable": {
            "Properties": [
                {
                    "CID": 2244,
                    "IUPACName": "2-acetoxybenzoic acid",
                    "MolecularFormula": "C9H8O4",
                }
            ]
        }
    }

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = _mock_response(mock_json)
        result = await resolver.resolve("aspirin", "Compound")

    assert result is not None
    assert result.canonical_id == "CID:2244"
    assert result.canonical_name == "2-acetoxybenzoic acid"
    assert result.resolver == "pubchem"
    assert result.metadata["molecular_formula"] == "C9H8O4"
    assert result.metadata["cid"] == "2244"


@pytest.mark.asyncio
async def test_pubchem_resolver_handles():
    """PubChem resolver only handles Compound/Drug/Molecule/Chemical labels."""
    resolver = PubChemResolver()
    assert resolver.handles("Compound") is True
    assert resolver.handles("Drug") is True
    assert resolver.handles("Molecule") is True
    assert resolver.handles("Chemical") is True
    assert resolver.handles("Protein") is False


# ---------------------------------------------------------------------------
# Composite resolver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_composite_routes_by_label():
    """CompositeResolver routes to correct resolver by label."""
    uniprot = UniProtResolver()
    mondo = MONDOResolver()
    pubchem = PubChemResolver()
    composite = CompositeResolver([uniprot, mondo, pubchem])

    # Mock UniProt for protein
    uniprot_json = {
        "results": [
            {
                "primaryAccession": "P00533",
                "proteinDescription": {
                    "recommendedName": {
                        "fullName": {"value": "Epidermal growth factor receptor"}
                    }
                },
                "organism": {"scientificName": "Homo sapiens"},
            }
        ]
    }

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = _mock_response(uniprot_json)
        result = await composite.resolve("EGFR", "Protein")

    assert result is not None
    assert result.resolver == "uniprot"
    assert result.canonical_id == "P00533"


@pytest.mark.asyncio
async def test_composite_returns_none_for_unhandled_label():
    """CompositeResolver returns None when no resolver handles the label."""
    composite = CompositeResolver([UniProtResolver(), MONDOResolver()])

    result = await composite.resolve("aspirin", "Compound")
    assert result is None


# ---------------------------------------------------------------------------
# Graceful failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolver_graceful_on_timeout():
    """Resolvers return None on timeout without crashing."""
    resolver = UniProtResolver()

    with patch(
        "httpx.AsyncClient.get",
        new_callable=AsyncMock,
        side_effect=httpx.TimeoutException("timed out"),
    ):
        result = await resolver.resolve("TP53", "Protein")

    assert result is None


@pytest.mark.asyncio
async def test_resolver_graceful_on_http_error():
    """Resolvers return None on HTTP errors without crashing."""
    resolver = MONDOResolver()

    with patch(
        "httpx.AsyncClient.get",
        new_callable=AsyncMock,
        side_effect=httpx.HTTPStatusError(
            "500",
            request=httpx.Request("GET", "https://example.com"),
            response=httpx.Response(500),
        ),
    ):
        result = await resolver.resolve("diabetes", "Disease")

    assert result is None


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_caching_avoids_duplicate_requests():
    """Second call for the same name uses cache, not network."""
    resolver = UniProtResolver()
    mock_json = {
        "results": [
            {
                "primaryAccession": "P04637",
                "proteinDescription": {
                    "recommendedName": {
                        "fullName": {"value": "Cellular tumor antigen p53"}
                    }
                },
                "organism": {"scientificName": "Homo sapiens"},
            }
        ]
    }

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = _mock_response(mock_json)

        result1 = await resolver.resolve("TP53", "Protein")
        result2 = await resolver.resolve("TP53", "Protein")

    # Only one HTTP call should have been made
    assert mock_get.call_count == 1
    assert result1 is not None
    assert result2 is not None
    assert result1.canonical_id == result2.canonical_id


# ---------------------------------------------------------------------------
# resolve_entities_enhanced
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enhanced_dedup_only():
    """With use_external=False, only name dedup runs."""
    results = [
        ExtractionResult(
            nodes=[
                {"id": "1", "name": "TP53", "label": "Protein"},
                {"id": "2", "name": "tp53", "label": "Protein"},
            ],
            edges=[{"source": "1", "target": "2", "type": "SELF"}],
            source_doc_id="doc1",
        )
    ]

    resolved = await resolve_entities_enhanced(results, use_external=False)

    # Should deduplicate by name
    assert len(resolved) == 1
    assert len(resolved[0].nodes) == 1
    assert resolved[0].nodes[0]["name"] == "TP53"
    # Edge should remap to surviving node
    assert resolved[0].edges[0]["source"] == "1"
    assert resolved[0].edges[0]["target"] == "1"


@pytest.mark.asyncio
async def test_enhanced_merges_by_canonical_id():
    """Entities resolving to same canonical ID get merged."""
    results = [
        ExtractionResult(
            nodes=[
                {"id": "1", "name": "p53", "label": "Protein"},
                {"id": "2", "name": "TP53 protein", "label": "Protein"},
                {"id": "3", "name": "diabetes", "label": "Disease"},
            ],
            edges=[
                {"source": "1", "target": "3", "type": "ASSOCIATED_WITH"},
                {"source": "2", "target": "3", "type": "ASSOCIATED_WITH"},
            ],
            source_doc_id="doc1",
        )
    ]

    # Both protein names resolve to the same UniProt accession
    resolved_diabetes = ResolvedEntity(
        canonical_id="MONDO:0005149",
        canonical_name="diabetes mellitus",
        source_names=["diabetes"],
        resolver="mondo",
        confidence=0.85,
        metadata={"synonyms": []},
    )

    call_count = 0

    async def mock_composite_resolve(name: str, label: str) -> ResolvedEntity | None:
        nonlocal call_count
        call_count += 1
        if label == "Protein":
            r = ResolvedEntity(
                canonical_id="P04637",
                canonical_name="Cellular tumor antigen p53",
                source_names=[name],
                resolver="uniprot",
                confidence=0.9,
                metadata={"organism": "Homo sapiens"},
            )
            return r
        if label == "Disease":
            return resolved_diabetes
        return None

    with patch(
        "graffold_ingest.resolvers.enhanced.CompositeResolver.resolve",
        side_effect=mock_composite_resolve,
    ):
        resolved = await resolve_entities_enhanced(results, use_external=True)

    # Two protein nodes should merge into one
    assert len(resolved) == 1
    nodes = resolved[0].nodes
    assert len(nodes) == 2  # 1 merged protein + 1 disease

    # Both edges should point to the surviving protein node
    edges = resolved[0].edges
    protein_node_id = next(n["id"] for n in nodes if n["label"] == "Protein")
    for edge in edges:
        if edge["type"] == "ASSOCIATED_WITH":
            assert edge["source"] == protein_node_id
