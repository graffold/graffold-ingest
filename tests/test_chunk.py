"""Tests for document chunking."""

from graffold_ingest.connectors.base import Document
from graffold_ingest.pipeline.chunk import chunk_documents


def _doc(content: str, id: str = "doc1") -> Document:
    return Document(id=id, content=content, source_url="http://x", source_type="web")


class TestChunkDocuments:
    def test_short_document_not_chunked(self):
        doc = _doc("Short text.")
        result = chunk_documents([doc], chunk_size=100)
        assert len(result) == 1
        assert result[0].id == "doc1"
        assert result[0].content == "Short text."

    def test_long_document_chunked(self):
        doc = _doc("a" * 500, id="long")
        result = chunk_documents([doc], chunk_size=200, overlap=50)
        assert len(result) > 1
        assert all(len(c.content) <= 200 for c in result)

    def test_chunk_ids_are_unique(self):
        doc = _doc("x" * 1000)
        result = chunk_documents([doc], chunk_size=200, overlap=50)
        ids = [c.id for c in result]
        assert len(ids) == len(set(ids))

    def test_overlap_creates_shared_content(self):
        doc = _doc("abcdefghij" * 50)  # 500 chars
        result = chunk_documents([doc], chunk_size=200, overlap=50)
        # The end of chunk 0 should overlap with start of chunk 1
        if len(result) >= 2:
            end_of_first = result[0].content[-50:]
            start_of_second = result[1].content[:50]
            assert end_of_first == start_of_second

    def test_preserves_metadata(self):
        doc = Document(
            id="m1", content="x" * 500, source_url="http://y",
            source_type="pdf", title="Paper", metadata={"author": "Alice"},
        )
        result = chunk_documents([doc], chunk_size=200, overlap=0)
        for chunk in result:
            assert chunk.source_url == "http://y"
            assert chunk.source_type == "pdf"
            assert chunk.title == "Paper"
            assert "author" in chunk.metadata

    def test_chunk_index_in_metadata(self):
        doc = _doc("a" * 1000)
        result = chunk_documents([doc], chunk_size=200, overlap=0)
        for i, chunk in enumerate(result):
            assert chunk.metadata["chunk_index"] == i

    def test_multiple_documents(self):
        docs = [_doc("short", id="a"), _doc("b" * 500, id="b")]
        result = chunk_documents(docs, chunk_size=200, overlap=0)
        assert any(c.id == "a" for c in result)  # short kept as-is
        assert any("b_chunk" in c.id for c in result)  # long chunked

    def test_empty_input(self):
        assert chunk_documents([]) == []

    def test_exact_chunk_size_not_chunked(self):
        doc = _doc("a" * 200)
        result = chunk_documents([doc], chunk_size=200)
        assert len(result) == 1
