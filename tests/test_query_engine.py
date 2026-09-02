"""Tests for the QueryEngine (cross-run knowledge retrieval)."""

import asyncio
from pathlib import Path

import pytest

from graffold_ingest.connectors.base import ExtractionResult
from graffold_ingest.pipeline.publish_parquet import publish_to_parquet
from graffold_ingest.query import QueryEngine


def _seed_graph(root: Path) -> None:
    """Write a small multi-program graph for testing."""
    # Program 1: crypto
    crypto = root / "crypto-v1"
    crypto.mkdir(parents=True)
    asyncio.run(publish_to_parquet(
        [ExtractionResult(
            nodes=[
                {"id": "target:cptrxr", "name": "CpTrxR", "label": "Target", "description": "thioredoxin reductase"},
                {"id": "drug:auranofin", "name": "Auranofin", "label": "Compound", "description": "gold compound"},
                {"id": "disease:crypto", "name": "Cryptosporidiosis", "label": "Disease"},
            ],
            edges=[
                {"source_id": "drug:auranofin", "target_id": "target:cptrxr", "type": "INHIBITS",
                 "description": "killed: gold toxicity"},
                {"source_id": "target:cptrxr", "target_id": "disease:crypto", "type": "TARGETS_DISEASE"},
            ],
            source_doc_id="crypto-run-1",
        )],
        output_dir=crypto,
    ))

    # Program 2: mastitis (shares IL-6-style biology)
    mastitis = root / "mastitis-v1"
    mastitis.mkdir(parents=True)
    asyncio.run(publish_to_parquet(
        [ExtractionResult(
            nodes=[
                {"id": "target:il6", "name": "IL-6", "label": "Target", "description": "inflammation"},
                {"id": "disease:mastitis", "name": "Mastitis", "label": "Disease"},
            ],
            edges=[
                {"source_id": "target:il6", "target_id": "disease:mastitis", "type": "TARGETS_DISEASE"},
            ],
            source_doc_id="mastitis-run-1",
        )],
        output_dir=mastitis,
    ))


class TestQueryEngine:
    def test_stats(self, tmp_path):
        _seed_graph(tmp_path)
        engine = QueryEngine(tmp_path)
        stats = engine.stats()
        assert stats["total_entities"] >= 5
        assert stats["total_relationships"] >= 3

    def test_loads_multiple_programs(self, tmp_path):
        _seed_graph(tmp_path)
        engine = QueryEngine(tmp_path)
        engine._load()
        assert len(engine._programs) == 2
        assert "crypto-v1" in engine._programs
        assert "mastitis-v1" in engine._programs

    def test_prior_knowledge_generates_doc(self, tmp_path):
        _seed_graph(tmp_path)
        engine = QueryEngine(tmp_path)
        doc = engine.prior_knowledge("cryptosporidiosis")
        assert isinstance(doc, str)
        assert "Prior Knowledge" in doc or "cryptosporidiosis" in doc.lower()

    def test_target_trajectory(self, tmp_path):
        _seed_graph(tmp_path)
        engine = QueryEngine(tmp_path)
        traj = engine.target_trajectory("CpTrxR")
        assert traj["mention_count"] >= 1
        assert "crypto-v1" in traj["programs"]

    def test_empty_graph(self, tmp_path):
        engine = QueryEngine(tmp_path)
        stats = engine.stats()
        assert stats["total_entities"] == 0
