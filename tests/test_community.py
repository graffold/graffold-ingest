"""Tests for Leiden community detection."""

from __future__ import annotations

import importlib.util

import pytest

from graffold_ingest.pipeline.community import (
    Community,
    CommunityResult,
    communities_to_records,
    detect_communities,
)

HAS_GRASPOLOGIC = importlib.util.find_spec("graspologic_native") is not None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _triangle_plus_isolated():
    """A triangle (A-B-C) and an isolated node (D)."""
    nodes = [
        {"id": "A", "name": "Alpha"},
        {"id": "B", "name": "Beta"},
        {"id": "C", "name": "Charlie"},
        {"id": "D", "name": "Delta"},
    ]
    edges = [
        {"source": "A", "target": "B", "type": "LINKS"},
        {"source": "B", "target": "C", "type": "LINKS"},
        {"source": "A", "target": "C", "type": "LINKS"},
    ]
    return nodes, edges


def _two_cliques():
    """Two triangles connected by a single bridge edge."""
    nodes = [{"id": f"n{i}", "name": f"Node{i}"} for i in range(6)]
    edges = [
        # Clique 1: n0-n1-n2
        {"source": "n0", "target": "n1"},
        {"source": "n1", "target": "n2"},
        {"source": "n0", "target": "n2"},
        # Clique 2: n3-n4-n5
        {"source": "n3", "target": "n4"},
        {"source": "n4", "target": "n5"},
        {"source": "n3", "target": "n5"},
        # Bridge
        {"source": "n2", "target": "n3"},
    ]
    return nodes, edges


# ---------------------------------------------------------------------------
# Tests requiring graspologic
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_GRASPOLOGIC, reason="graspologic not installed")
class TestDetectCommunities:
    """Tests for detect_communities with graspologic available."""

    def test_basic_triangle_and_isolated(self):
        """Triangle + isolated node should yield at least 2 communities."""
        nodes, edges = _triangle_plus_isolated()
        result = detect_communities(nodes, edges, resolutions=[1.0])

        assert isinstance(result, CommunityResult)
        assert result.num_levels == 1
        assert len(result.communities) >= 2

        # Isolated node D should be in its own community
        d_communities = result.node_assignments["D"]
        assert len(d_communities) == 1

        # Triangle nodes should share a community
        a_comm = result.node_assignments["A"][0]
        b_comm = result.node_assignments["B"][0]
        c_comm = result.node_assignments["C"][0]
        assert a_comm == b_comm == c_comm

        # D should be separate from the triangle
        assert d_communities[0] != a_comm

    def test_multi_level_produces_hierarchy(self):
        """Multiple resolutions produce multiple levels with parent links."""
        nodes, edges = _two_cliques()
        result = detect_communities(nodes, edges, resolutions=[1.5, 0.5, 0.1])

        assert result.num_levels == 3
        assert len(result.modularity_scores) == 3

        # Each node should have assignments at each level
        for nid in [n["id"] for n in nodes]:
            assert len(result.node_assignments[nid]) == 3

        # At the coarsest level (low resolution), expect fewer communities
        coarse_communities = [c for c in result.communities if c.level == 2]
        fine_communities = [c for c in result.communities if c.level == 0]
        assert len(coarse_communities) <= len(fine_communities)

    def test_two_cliques_detected(self):
        """Two well-separated cliques should be in different communities."""
        nodes, edges = _two_cliques()
        result = detect_communities(nodes, edges, resolutions=[1.5])

        # At high resolution, the two cliques should separate
        n0_comm = result.node_assignments["n0"][0]
        n5_comm = result.node_assignments["n5"][0]
        # Nodes within same clique should be together
        assert result.node_assignments["n1"][0] == n0_comm
        assert result.node_assignments["n4"][0] == n5_comm
        # Cliques should be separate
        assert n0_comm != n5_comm

    def test_parent_links_populated(self):
        """Communities at finer levels should have parent_id set."""
        nodes, edges = _two_cliques()
        result = detect_communities(nodes, edges, resolutions=[1.5, 0.3])

        level_0 = [c for c in result.communities if c.level == 0]
        # At least some level-0 communities should have a parent
        parents = [c.parent_id for c in level_0 if c.parent_id is not None]
        assert len(parents) > 0

    def test_seed_reproducibility(self):
        """Same seed produces identical results."""
        nodes, edges = _two_cliques()
        r1 = detect_communities(nodes, edges, seed=123)
        r2 = detect_communities(nodes, edges, seed=123)

        assert r1.node_assignments == r2.node_assignments
        assert r1.modularity_scores == r2.modularity_scores

    def test_source_id_target_id_keys(self):
        """Edges with 'source_id'/'target_id' keys also work."""
        nodes = [
            {"id": "X", "name": "X"},
            {"id": "Y", "name": "Y"},
            {"id": "Z", "name": "Z"},
        ]
        edges = [
            {"source_id": "X", "target_id": "Y"},
            {"source_id": "Y", "target_id": "Z"},
            {"source_id": "X", "target_id": "Z"},
        ]
        result = detect_communities(nodes, edges, resolutions=[1.0])
        # All three should be in same community (complete graph)
        assert (
            result.node_assignments["X"][0]
            == result.node_assignments["Y"][0]
            == result.node_assignments["Z"][0]
        )

    def test_weighted_edges(self):
        """Weighted edges are respected."""
        nodes = [{"id": f"n{i}", "name": f"N{i}"} for i in range(4)]
        edges = [
            {"source": "n0", "target": "n1", "weight": 10.0},
            {"source": "n2", "target": "n3", "weight": 10.0},
            {"source": "n1", "target": "n2", "weight": 0.01},
        ]
        result = detect_communities(nodes, edges, resolutions=[1.5])
        # Strong pairs should be in same community
        assert result.node_assignments["n0"][0] == result.node_assignments["n1"][0]
        assert result.node_assignments["n2"][0] == result.node_assignments["n3"][0]


# ---------------------------------------------------------------------------
# Tests that don't require graspologic
# ---------------------------------------------------------------------------


class TestEmptyGraph:
    """Tests for edge cases."""

    def test_empty_nodes_returns_empty(self):
        """Empty input returns empty result."""
        if not HAS_GRASPOLOGIC:
            pytest.skip("graspologic not installed")
        result = detect_communities([], [])
        assert result.communities == []
        assert result.node_assignments == {}
        assert result.num_levels == 0

    def test_no_edges_single_nodes(self):
        """Nodes with no edges each get their own community."""
        if not HAS_GRASPOLOGIC:
            pytest.skip("graspologic not installed")
        nodes = [{"id": "A"}, {"id": "B"}, {"id": "C"}]
        result = detect_communities(nodes, [], resolutions=[1.0])
        assert result.num_levels == 1
        # Each node in its own community
        a_comm = result.node_assignments["A"][0]
        b_comm = result.node_assignments["B"][0]
        c_comm = result.node_assignments["C"][0]
        assert a_comm != b_comm
        assert b_comm != c_comm


class TestGracefulFallback:
    """Test behavior when graspologic is unavailable."""

    def test_runtime_error_without_graspologic(self, monkeypatch):
        """detect_communities raises RuntimeError if leiden is None."""
        import graffold_ingest.pipeline.community as mod

        monkeypatch.setattr(mod, "leiden", None)

        with pytest.raises(RuntimeError, match="graspologic is not installed"):
            detect_communities([{"id": "A"}], [{"source": "A", "target": "A"}])


class TestCommunitiesToRecords:
    """Tests for the Parquet export helper."""

    def test_output_format(self):
        """Records have expected keys and values."""
        communities = [
            Community(
                id="c1",
                level=0,
                title="Test",
                summary="A test community",
                member_ids=["A", "B"],
                parent_id="c2",
                size=2,
            ),
            Community(
                id="c2",
                level=1,
                title="Parent",
                summary="Parent community",
                member_ids=["A", "B", "C"],
                parent_id=None,
                size=3,
            ),
        ]
        result = CommunityResult(
            communities=communities,
            node_assignments={"A": ["c1", "c2"], "B": ["c1", "c2"], "C": ["c2"]},
            num_levels=2,
            modularity_scores=[0.4, 0.2],
        )

        records = communities_to_records(result)

        assert len(records) == 2
        assert records[0]["community_id"] == "c1"
        assert records[0]["level"] == 0
        assert records[0]["title"] == "Test"
        assert records[0]["summary"] == "A test community"
        assert records[0]["member_ids"] == ["A", "B"]
        assert records[0]["member_count"] == 2
        assert records[0]["parent_id"] == "c2"

        assert records[1]["community_id"] == "c2"
        assert records[1]["parent_id"] is None

    def test_empty_result(self):
        """Empty CommunityResult produces empty records."""
        result = CommunityResult(communities=[], node_assignments={}, num_levels=0)
        assert communities_to_records(result) == []
