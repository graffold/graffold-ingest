"""Tests for the query agent (5-phase graph-grounded QA)."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from graffold_ingest.pipeline.query_agent import query_graph, QueryResult, _extract_search_terms


class TestExtractSearchTerms:
    def test_extracts_entity_names(self):
        terms = _extract_search_terms("What does TP53 inhibit?")
        assert "TP53" in terms
        assert "inhibit" in terms

    def test_filters_stopwords(self):
        terms = _extract_search_terms("What is the relationship between BRCA1 and cancer?")
        assert "What" not in terms and "what" not in terms
        assert "BRCA1" in terms
        assert "cancer" in terms

    def test_short_tokens_filtered(self):
        terms = _extract_search_terms("Is A a B?")
        # single-char tokens filtered
        assert "A" not in terms


class TestQueryGraph:
    @pytest.fixture
    def mock_backend(self):
        backend = AsyncMock()
        backend.name = "mock"
        backend.query_entities = AsyncMock(return_value=[
            {"id": "tp53", "name": "TP53", "type": "Protein"},
            {"id": "mdm2", "name": "MDM2", "type": "Protein"},
        ])
        backend.get_neighbors = AsyncMock(return_value={
            "neighbors": [{"id": "mdm2", "name": "MDM2", "type": "Protein"}]
        })
        return backend

    @pytest.mark.asyncio
    async def test_returns_query_result(self, mock_backend):
        with patch(
            "graffold_ingest.pipeline.query_agent._call_llm",
            new_callable=AsyncMock,
            return_value="TP53 inhibits MDM2 via direct binding.",
        ):
            result = await query_graph(
                "What does TP53 inhibit?",
                backend=mock_backend,
            )
        assert isinstance(result, QueryResult)
        assert "TP53" in result.answer or "MDM2" in result.answer
        assert result.total_seconds > 0
        assert "discovery" in result.phases
        assert "synthesis" in result.phases

    @pytest.mark.asyncio
    async def test_includes_entities(self, mock_backend):
        with patch(
            "graffold_ingest.pipeline.query_agent._call_llm",
            new_callable=AsyncMock,
            return_value="Answer based on graph.",
        ):
            result = await query_graph("TP53?", backend=mock_backend)
        assert len(result.entities) >= 1

    @pytest.mark.asyncio
    async def test_handles_empty_graph(self):
        empty_backend = AsyncMock()
        empty_backend.name = "empty"
        empty_backend.query_entities = AsyncMock(return_value=[])
        empty_backend.get_neighbors = AsyncMock(return_value={"neighbors": []})

        with patch(
            "graffold_ingest.pipeline.query_agent._call_llm",
            new_callable=AsyncMock,
            return_value="Insufficient graph coverage.",
        ):
            result = await query_graph("Unknown entity?", backend=empty_backend)
        assert "Insufficient" in result.answer or len(result.entities) == 0

    @pytest.mark.asyncio
    async def test_verify_flag_controls_phase5(self, mock_backend):
        with patch(
            "graffold_ingest.pipeline.query_agent._call_llm",
            new_callable=AsyncMock,
            return_value="Some answer.",
        ):
            result = await query_graph(
                "TP53?", backend=mock_backend, verify=False
            )
        assert "verification" not in result.phases

    @pytest.mark.asyncio
    async def test_llm_failure_graceful(self, mock_backend):
        with patch(
            "graffold_ingest.pipeline.query_agent._call_llm",
            new_callable=AsyncMock,
            side_effect=RuntimeError("LLM down"),
        ):
            result = await query_graph("TP53?", backend=mock_backend)
        # Should not crash, produces fallback answer
        assert "failed" in result.answer.lower() or "entities" in result.answer.lower()
