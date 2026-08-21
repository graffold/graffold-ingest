"""Tests for connectors."""

import pytest

from graffold_ingest.connectors.base import Document, Connector
from graffold_ingest.connectors import CONNECTORS


class TestConnectorRegistry:
    def test_all_expected_connectors_registered(self):
        expected = {"web", "pdf", "api", "csv", "database"}
        assert expected.issubset(set(CONNECTORS.keys()))

    def test_each_connector_implements_protocol(self):
        for name, cls in CONNECTORS.items():
            instance = cls()
            assert isinstance(instance, Connector), f"{name} doesn't implement Connector"
            assert instance.name() == name


class TestDocumentModel:
    def test_document_creation(self):
        doc = Document(id="d1", content="hello", source_type="web")
        assert doc.id == "d1"
        assert doc.content == "hello"
        assert doc.metadata == {}

    def test_document_with_metadata(self):
        doc = Document(
            id="d2", content="x",
            metadata={"key": "value"}, source_url="http://x",
        )
        assert doc.metadata["key"] == "value"
        assert doc.source_url == "http://x"

    def test_document_chunk_id_optional(self):
        doc = Document(id="d3", content="y")
        assert doc.chunk_id is None
