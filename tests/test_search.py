"""Tests for DRIFT and Global search algorithms."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

from graffold_ingest.pipeline.drift_search import (
    DriftResult,
    _extract_search_terms,
    _format_context,
    drift_search,
)
from graffold_ingest.pipeline.global_search import (
    GlobalSearchResult,
    global_search,
)

# ─── DRIFT search tests ──────────────────────────────────────────────────────


class TestDriftHelpers:
    def test_extract_search_terms_filters_stopwords(self):
        terms = _extract_search_terms("What are the effects of 3-NOP on methanogenesis?")
        assert "what" not in terms
        assert "the" not in terms
        assert "3-NOP" in terms or "3-nop" in [t.lower() for t in terms]
        assert "methanogenesis" in terms

    def test_extract_search_terms_limits_to_10(self):
        long_query = " ".join(f"term{i}" for i in range(20))
        terms = _extract_search_terms(long_query)
        assert len(terms) <= 10

    def test_format_context_entities(self):
        context = {
            "entities": [
                {"name": "3-NOP", "type": "Compound", "description": "MCR inhibitor"},
                {"name": "Methanogenesis", "type": "Process", "description": ""},
            ],
            "relationships": [
                {"source": "3-NOP", "target": "Methanogenesis", "type": "INHIBITS"},
            ],
        }
        formatted = _format_context(context)
        assert "3-NOP" in formatted
        assert "Compound" in formatted
        assert "MCR inhibitor" in formatted
        assert "INHIBITS" in formatted

    def test_format_context_deduplicates_relationships(self):
        context = {
            "entities": [{"name": "A", "type": "X", "description": ""}],
            "relationships": [
                {"source": "A", "target": "B", "type": "R"},
                {"source": "A", "target": "B", "type": "R"},  # duplicate
            ],
        }
        formatted = _format_context(context)
        assert formatted.count("A —[R]→ B") == 1


@patch("graffold_ingest.pipeline.drift_search._local_entity_search", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.extract._call_llm", new_callable=AsyncMock)
def test_drift_search_basic(mock_llm, mock_search):
    """DRIFT search finds entities, evaluates, and reduces."""
    import asyncio

    # Prime returns sub-questions
    mock_llm.side_effect = [
        json.dumps(["What is 3-NOP?", "How does it affect methane?"]),
        # Evaluate call 1 — can answer
        json.dumps({
            "can_answer": True,
            "answer": "3-NOP inhibits methanogenesis via MCR",
            "follow_ups": [],
            "confidence": 0.8,
        }),
        # Evaluate call 2 — can answer
        json.dumps({
            "can_answer": True,
            "answer": "Reduces methane by 20-30%",
            "follow_ups": [],
            "confidence": 0.7,
        }),
        # Reduce
        "3-NOP inhibits methanogenesis via MCR, reducing methane by 20-30%.",
    ]

    mock_search.return_value = {
        "entities": [{"id": "t:3nop", "name": "3-NOP", "type": "Compound", "description": ""}],
        "relationships": [{"source": "3-NOP", "target": "MCR", "type": "INHIBITS"}],
        "entity_ids": {"t:3nop"},
    }

    result = asyncio.run(drift_search(
        "What does 3-NOP do?",
        database_uri="bolt://fake:7687",
        llm_service="ollama",
    ))

    assert isinstance(result, DriftResult)
    assert result.hops > 0
    assert len(result.intermediate_answers) >= 1
    assert result.confidence > 0
    assert "3-NOP" in result.answer or "methane" in result.answer.lower()


@patch("graffold_ingest.pipeline.drift_search._local_entity_search", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.extract._call_llm", new_callable=AsyncMock)
def test_drift_search_empty_graph(mock_llm, mock_search):
    """DRIFT search handles empty graph gracefully."""
    import asyncio

    mock_llm.return_value = json.dumps(["sub-question"])
    mock_search.return_value = {"entities": [], "relationships": [], "entity_ids": set()}

    result = asyncio.run(drift_search(
        "Unknown topic",
        database_uri="bolt://fake:7687",
        llm_service="ollama",
    ))

    assert isinstance(result, DriftResult)
    assert "Insufficient" in result.answer
    assert result.confidence == 0.0


# ─── Global search tests ──────────────────────────────────────────────────────


@patch("graffold_ingest.pipeline.global_search._load_communities_from_neo4j", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.extract._call_llm", new_callable=AsyncMock)
def test_global_search_basic(mock_llm, mock_load):
    """Global search maps over communities and reduces."""
    import asyncio

    mock_load.return_value = [
        {"id": "c1", "title": "Methane inhibitors", "summary": "3-NOP and Bromoform reduce CH4", "level": 0, "size": 5},
        {"id": "c2", "title": "Feed additives", "summary": "Various supplements for cattle", "level": 0, "size": 3},
    ]

    # Map calls (one per community)
    mock_llm.side_effect = [
        json.dumps({"score": 8, "key_points": ["3-NOP reduces methane by 30%"], "entities_mentioned": ["3-NOP"]}),
        json.dumps({"score": 2, "key_points": [], "entities_mentioned": []}),
        # Reduce call
        "3-NOP is the most effective methane inhibitor, reducing emissions by 30%.",
    ]

    result = asyncio.run(global_search(
        "What are the best methane inhibitors?",
        database_uri="bolt://fake:7687",
        llm_service="ollama",
    ))

    assert isinstance(result, GlobalSearchResult)
    assert result.communities_consulted == 2
    assert result.communities_relevant == 1
    assert "3-NOP" in result.entities_mentioned
    assert "methane" in result.answer.lower() or "3-NOP" in result.answer


@patch("graffold_ingest.pipeline.global_search._load_communities_from_neo4j", new_callable=AsyncMock)
def test_global_search_no_communities(mock_load):
    """Global search handles no communities gracefully."""
    import asyncio

    mock_load.return_value = []

    result = asyncio.run(global_search(
        "Anything",
        database_uri="bolt://fake:7687",
        llm_service="ollama",
    ))

    assert isinstance(result, GlobalSearchResult)
    assert result.communities_consulted == 0
    assert "No community summaries" in result.answer


@patch("graffold_ingest.pipeline.global_search._load_communities_from_neo4j", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.extract._call_llm", new_callable=AsyncMock)
def test_global_search_no_relevant_communities(mock_llm, mock_load):
    """Global search handles when no communities are relevant."""
    import asyncio

    mock_load.return_value = [
        {"id": "c1", "title": "Unrelated topic", "summary": "Something about software", "level": 0, "size": 5},
    ]

    mock_llm.return_value = json.dumps({"score": 1, "key_points": [], "entities_mentioned": []})

    result = asyncio.run(global_search(
        "Methane inhibitors",
        database_uri="bolt://fake:7687",
        llm_service="ollama",
    ))

    assert result.communities_relevant == 0
    assert "do not contain sufficient" in result.answer
