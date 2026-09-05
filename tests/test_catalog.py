"""Tests for the graph catalog."""

import asyncio
from pathlib import Path

from graffold_ingest.catalog import scan, to_manifest, to_markdown, to_html, _classify
from graffold_ingest.connectors.base import ExtractionResult
from graffold_ingest.pipeline.publish_parquet import publish_to_parquet


def _make_graph(d: Path, nodes, edges):
    d.mkdir(parents=True, exist_ok=True)
    asyncio.run(publish_to_parquet(
        [ExtractionResult(nodes=nodes, edges=edges, source_doc_id="t")],
        output_dir=d, run_id="t"))


class TestClassify:
    def test_kinds(self):
        assert _classify("etec-pigs-harmonized") == "harmonized"
        assert _classify("etec-pigs-clean") == "clean"
        assert _classify("master") == "master"
        assert _classify("atlas-full") == "atlas"
        assert _classify("elanco-coccidiosis") == "graph"


class TestScan:
    def test_scan_and_counts(self, tmp_path):
        _make_graph(tmp_path / "g1", [
            {"id": "a", "name": "TP53", "type": "Target"},
            {"id": "b", "name": "MDM2", "type": "Target"},
            {"id": "c", "name": "cancer", "type": "Disease"},
        ], [{"source_id": "a", "target_id": "b", "type": "INHIBITS"}])

        entries = scan(tmp_path)
        assert len(entries) == 1
        e = entries[0]
        assert e.name == "g1"
        assert e.entities == 3
        assert e.relationships == 1
        assert e.types["Target"] == 2

    def test_program_membership_parsed(self, tmp_path):
        _make_graph(tmp_path / "m", [
            {"id": "x", "name": "shared", "type": "Target",
             "description": "foo [programs:alltech,elanco]"},
        ], [])
        entries = scan(tmp_path)
        assert entries[0].programs == ["alltech", "elanco"]

    def test_filter(self, tmp_path):
        _make_graph(tmp_path / "keep-me", [{"id": "a", "name": "A", "type": "T"}], [])
        _make_graph(tmp_path / "skip-me", [{"id": "b", "name": "B", "type": "T"}], [])
        entries = scan(tmp_path, filt=["keep"])
        assert len(entries) == 1
        assert entries[0].name == "keep-me"

    def test_empty_dir(self, tmp_path):
        assert scan(tmp_path) == []


class TestRender:
    def test_manifest(self, tmp_path):
        _make_graph(tmp_path / "g", [{"id": "a", "name": "A", "type": "T"}], [])
        m = to_manifest(scan(tmp_path))
        assert m["total_graphs"] == 1
        assert m["total_entities"] == 1

    def test_markdown_and_html(self, tmp_path):
        _make_graph(tmp_path / "g", [{"id": "a", "name": "A", "type": "Target"}], [])
        entries = scan(tmp_path)
        md = to_markdown(entries)
        assert "Graph Catalog" in md and "`g`" in md
        html = to_html(entries)
        assert "<table>" in html and "Graffold Graph Catalog" in html
