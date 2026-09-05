"""Tests for the FalkorDB backend.

Skipped automatically if no FalkorDB is reachable at localhost:6379.
"""

import asyncio
import uuid

import pytest

from graffold_ingest.backends import get_backend
from graffold_ingest.backends.falkordb import FalkorDBBackend, _safe_label
from graffold_ingest.connectors.base import ExtractionResult


def _falkor_available() -> bool:
    try:
        b = get_backend("falkordb", graph_name="ping")
        return asyncio.run(b.health_check())
    except Exception:
        return False


falkor = pytest.mark.skipif(not _falkor_available(), reason="no FalkorDB at localhost:6379")


class TestSafeLabel:
    def test_sanitizes(self):
        assert _safe_label("ASSOCIATED_WITH") == "ASSOCIATED_WITH"
        assert _safe_label("rel-type/x") == "rel_type_x"
        assert _safe_label("123bad").startswith("E_")
        assert _safe_label("") == "Entity"

    def test_protocol_conformance(self):
        from graffold_ingest.backends import GraphBackend
        b = FalkorDBBackend(graph_name="x")
        assert isinstance(b, GraphBackend)
        assert b.name == "falkordb"


@falkor
class TestFalkorDBRoundtrip:
    def _graph_name(self):
        return f"test-{uuid.uuid4().hex[:8]}"

    def test_publish_query_neighbors(self):
        name = self._graph_name()
        b = get_backend("falkordb", graph_name=name)
        r = [ExtractionResult(
            nodes=[
                {"id": "a", "name": "TP53", "label": "Protein", "type": "Protein"},
                {"id": "b", "name": "MDM2", "label": "Protein", "type": "Protein"},
                {"id": "c", "name": "cancer", "label": "Disease", "type": "Disease"},
            ],
            edges=[
                {"source_id": "a", "target_id": "b", "type": "INHIBITS"},
                {"source_id": "a", "target_id": "c", "type": "ASSOCIATED_WITH"},
            ],
            source_doc_id="t",
        )]
        counts = asyncio.run(b.publish(r))
        assert counts["nodes_created"] == 3
        assert counts["edges_created"] == 2

        found = asyncio.run(b.query_entities("TP53"))
        assert any(x["name"] == "TP53" for x in found)

        nb = asyncio.run(b.get_neighbors("a"))
        names = {x["name"] for x in nb["neighbors"]}
        assert "MDM2" in names and "cancer" in names

        # cleanup
        from falkordb import FalkorDB
        FalkorDB().select_graph(name).delete()

    def test_named_graphs_isolated(self):
        n1, n2 = self._graph_name(), self._graph_name()
        b1 = get_backend("falkordb", graph_name=n1)
        b2 = get_backend("falkordb", graph_name=n2)
        asyncio.run(b1.publish([ExtractionResult(
            nodes=[{"id": "x", "name": "OnlyInOne", "type": "T"}], edges=[], source_doc_id="1")]))
        # b2 should not see b1's node
        found = asyncio.run(b2.query_entities("OnlyInOne"))
        assert found == []
        from falkordb import FalkorDB
        FalkorDB().select_graph(n1).delete()
