"""Tests for the GraphBackend protocol and registry."""

import pytest

from graffold_ingest.backends import (
    GraphBackend,
    get_backend,
    register_backend,
    _BACKENDS,
)
from graffold_ingest.backends.neo4j import Neo4jBackend
from graffold_ingest.backends.neptune import NeptuneBackend
from graffold_ingest.backends.duckdb import DuckDBBackend
from graffold_ingest.backends.spanner import SpannerGraphBackend


class TestProtocolConformance:
    """Verify all backends implement the GraphBackend protocol."""

    def test_neo4j_is_graph_backend(self):
        backend = Neo4jBackend()
        assert isinstance(backend, GraphBackend)

    def test_neptune_is_graph_backend(self):
        backend = NeptuneBackend(endpoint="test.neptune.amazonaws.com")
        assert isinstance(backend, GraphBackend)

    def test_duckdb_is_graph_backend(self):
        backend = DuckDBBackend(parquet_dir="/tmp/test")
        assert isinstance(backend, GraphBackend)

    def test_spanner_is_graph_backend(self):
        backend = SpannerGraphBackend(instance_id="test", database_id="db")
        assert isinstance(backend, GraphBackend)


class TestBackendNames:
    def test_neo4j_name(self):
        assert Neo4jBackend().name == "neo4j"

    def test_neptune_name(self):
        assert NeptuneBackend().name == "neptune"

    def test_duckdb_name(self):
        assert DuckDBBackend().name == "duckdb"

    def test_spanner_name(self):
        assert SpannerGraphBackend(instance_id="i", database_id="d").name == "spanner"


class TestRegistry:
    def test_get_backend_neo4j(self):
        backend = get_backend("neo4j")
        assert backend.name == "neo4j"

    def test_get_backend_neptune(self):
        backend = get_backend("neptune")
        assert backend.name == "neptune"

    def test_get_backend_duckdb(self):
        backend = get_backend("duckdb")
        assert backend.name == "duckdb"

    def test_get_backend_spanner(self):
        backend = get_backend("spanner")
        assert backend.name == "spanner"

    def test_get_backend_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown backend"):
            get_backend("cosmosdb")

    def test_register_custom_backend(self):
        class FakeBackend:
            @property
            def name(self):
                return "fake"

            async def publish(self, results, **kwargs):
                return {}

            async def query_entities(self, search_term, *, limit=10):
                return []

            async def get_neighbors(self, entity_id, *, max_hops=1):
                return {}

            async def health_check(self):
                return True

        register_backend("fake", FakeBackend)
        backend = get_backend("fake")
        assert backend.name == "fake"
        # Cleanup
        del _BACKENDS["fake"]


class TestDuckDBBackend:
    @pytest.mark.asyncio
    async def test_health_check_nonexistent_dir(self):
        backend = DuckDBBackend(parquet_dir="/tmp/nonexistent_xyz_graffold")
        result = await backend.health_check()
        assert result is False

    @pytest.mark.asyncio
    async def test_health_check_existing_dir(self, tmp_path):
        backend = DuckDBBackend(parquet_dir=str(tmp_path))
        result = await backend.health_check()
        assert result is True

    @pytest.mark.asyncio
    async def test_query_entities_empty_dir(self, tmp_path):
        backend = DuckDBBackend(parquet_dir=str(tmp_path))
        result = await backend.query_entities("TP53")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_neighbors_empty_dir(self, tmp_path):
        backend = DuckDBBackend(parquet_dir=str(tmp_path))
        result = await backend.get_neighbors("entity1")
        assert result == {"neighbors": []}

    @pytest.mark.asyncio
    async def test_publish_writes_parquet(self, tmp_path):
        from graffold_ingest.connectors.base import ExtractionResult

        backend = DuckDBBackend(parquet_dir=str(tmp_path))
        results = [
            ExtractionResult(
                nodes=[{"id": "n1", "label": "Protein", "name": "TP53"}],
                edges=[],
                source_doc_id="doc1",
            )
        ]
        counts = await backend.publish(results)
        assert counts["entities_written"] >= 1
        assert (tmp_path / "entities.parquet").exists()

    @pytest.mark.asyncio
    async def test_roundtrip_publish_then_query(self, tmp_path):
        from graffold_ingest.connectors.base import ExtractionResult

        backend = DuckDBBackend(parquet_dir=str(tmp_path))
        results = [
            ExtractionResult(
                nodes=[
                    {"id": "tp53", "label": "Protein", "name": "TP53"},
                    {"id": "mdm2", "label": "Protein", "name": "MDM2"},
                ],
                edges=[{"source_id": "tp53", "target_id": "mdm2", "type": "INHIBITS"}],
                source_doc_id="doc1",
            )
        ]
        await backend.publish(results)
        entities = await backend.query_entities("TP53")
        assert len(entities) >= 1
        assert entities[0]["name"] == "TP53"

    @pytest.mark.asyncio
    async def test_get_neighbors_after_publish(self, tmp_path):
        from graffold_ingest.connectors.base import ExtractionResult

        backend = DuckDBBackend(parquet_dir=str(tmp_path))
        results = [
            ExtractionResult(
                nodes=[
                    {"id": "tp53", "label": "Protein", "name": "TP53"},
                    {"id": "mdm2", "label": "Protein", "name": "MDM2"},
                ],
                edges=[{"source_id": "tp53", "target_id": "mdm2", "type": "INHIBITS"}],
                source_doc_id="doc1",
            )
        ]
        await backend.publish(results)
        neighbors = await backend.get_neighbors("tp53", max_hops=1)
        assert len(neighbors["neighbors"]) >= 1
        names = [n["name"] for n in neighbors["neighbors"]]
        assert "MDM2" in names
