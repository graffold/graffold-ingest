"""Tests for LLM entity extraction."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from graffold_ingest.connectors.base import Document
from graffold_ingest.pipeline.extract import extract_entities


def _doc(content: str = "TP53 inhibits MDM2 in cancer.", id: str = "doc1") -> Document:
    return Document(id=id, content=content, source_url="http://x", source_type="web")


LLM_RESPONSE = json.dumps({
    "nodes": [
        {"id": "tp53", "label": "Protein", "name": "TP53", "properties": {}},
        {"id": "mdm2", "label": "Protein", "name": "MDM2", "properties": {}},
    ],
    "edges": [
        {"source": "tp53", "target": "mdm2", "type": "INHIBITS", "properties": {}}
    ],
})


@pytest.fixture
def mock_llm():
    with patch(
        "graffold_ingest.pipeline.extract._call_llm",
        new_callable=AsyncMock,
        return_value=LLM_RESPONSE,
    ) as m:
        yield m


@pytest.mark.asyncio
async def test_extract_returns_nodes_and_edges(mock_llm):
    results = await extract_entities([_doc()])
    assert len(results) == 1
    assert len(results[0].nodes) == 2
    assert len(results[0].edges) == 1
    assert results[0].source_doc_id == "doc1"


@pytest.mark.asyncio
async def test_extract_multiple_documents(mock_llm):
    docs = [_doc(id="a"), _doc(id="b"), _doc(id="c")]
    results = await extract_entities(docs)
    assert len(results) == 3
    assert [r.source_doc_id for r in results] == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_extract_graceful_on_llm_failure():
    with patch(
        "graffold_ingest.pipeline.extract._call_llm",
        new_callable=AsyncMock,
        side_effect=RuntimeError("LLM down"),
    ):
        results = await extract_entities([_doc()])
        assert len(results) == 1
        assert results[0].nodes == []
        assert results[0].edges == []
        assert results[0].source_doc_id == "doc1"


@pytest.mark.asyncio
async def test_extract_graceful_on_bad_json():
    with patch(
        "graffold_ingest.pipeline.extract._call_llm",
        new_callable=AsyncMock,
        return_value="not json at all {{{",
    ):
        results = await extract_entities([_doc()])
        assert len(results) == 1
        assert results[0].nodes == []


@pytest.mark.asyncio
async def test_extract_truncates_long_content(mock_llm):
    long_doc = _doc(content="x" * 20000)
    await extract_entities([long_doc])
    # The prompt should have truncated content to 8000 chars
    call_args = mock_llm.call_args[0][0]
    # Prompt template ~1100 chars + 8000 content = ~9100 max
    assert len(call_args) < 9500


@pytest.mark.asyncio
async def test_extract_passes_service_and_model(mock_llm):
    await extract_entities([_doc()], llm_service="ollama", model_id="qwen3:1.7b")
    mock_llm.assert_called_once()
    _, service, model = mock_llm.call_args[0]
    assert service == "ollama"
    assert model == "qwen3:1.7b"


@pytest.mark.asyncio
async def test_extract_empty_docs():
    results = await extract_entities([])
    assert results == []
