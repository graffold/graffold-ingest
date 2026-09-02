"""Tests for the Agteria connector."""

import asyncio
import tempfile
from pathlib import Path

from graffold_ingest.connectors.agteria import AgteriaConnector


def test_connector_name():
    assert AgteriaConnector().name() == "agteria"


def test_fetch_phase_files():
    with tempfile.TemporaryDirectory() as tmp:
        (Path(tmp) / "phase-1-disease-map.md").write_text("# Phase 1\nContent")
        (Path(tmp) / "phase-4-kill-report.md").write_text("# Phase 4\nKills")
        (Path(tmp) / "not-a-phase.txt").write_text("ignored")

        connector = AgteriaConnector()
        docs = asyncio.run(connector.fetch(path=tmp))
        assert len(docs) == 2
        assert all(d.source_type == "agteria" for d in docs)


def test_extract_direct_target_table():
    with tempfile.TemporaryDirectory() as tmp:
        (Path(tmp) / "phase-4-kill-report.md").write_text("""\
# Phase 4 — Kill Report: Test Disease

| Registry id | Target | Status |
|---|---|---|
| KILL-001 | TargetA | Dead |
| KILL-002 | TargetB | Alive |
| T-010 | TargetC | Wounded |
""")
        connector = AgteriaConnector()
        results = asyncio.run(
            connector.extract_direct(path=tmp, phases=["phase-4-kill-report.md"])
        )
        assert len(results) == 1
        ids = {n["id"] for n in results[0].nodes}
        assert "KILL-001" in ids
        assert "KILL-002" in ids
        assert "T-010" in ids
        assert any(n["label"] == "Disease" for n in results[0].nodes)
        assert any(e["type"] == "TARGETS_DISEASE" for e in results[0].edges)


def test_extract_direct_candidate_headers():
    with tempfile.TemporaryDirectory() as tmp:
        (Path(tmp) / "phase-3-candidates.md").write_text("""\
# Phase 3 — Candidates: Bovine Disease

## C1: Novel Compound X (gut-restricted)

Some description.

## C2: Parasite TrxR Inhibitor (non-gold)

More text.
""")
        connector = AgteriaConnector()
        results = asyncio.run(
            connector.extract_direct(path=tmp, phases=["phase-3-candidates.md"])
        )
        assert len(results) == 1
        names = {n["name"] for n in results[0].nodes}
        assert "C1: Novel Compound X" in names
        assert "C2: Parasite TrxR Inhibitor" in names


def test_extract_direct_re_proposed_relationships():
    with tempfile.TemporaryDirectory() as tmp:
        (Path(tmp) / "phase-3-candidates.md").write_text("""\
# Phase 3 — Test

| Registry id | Killed molecule | Target re-proposed as |
|---|---|---|
| KILL-087 | Lapaquistat | host FDFT1 (C1) |
| KILL-090 | Auranofin | parasite CpTrxR (C2) |
""")
        connector = AgteriaConnector()
        results = asyncio.run(
            connector.extract_direct(path=tmp, phases=["phase-3-candidates.md"])
        )
        re_proposed = [e for e in results[0].edges if e["type"] == "RE_PROPOSED_AS"]
        assert len(re_proposed) == 2
        assert re_proposed[0]["source_id"] == "KILL-087"


def test_empty_directory():
    with tempfile.TemporaryDirectory() as tmp:
        connector = AgteriaConnector()
        docs = asyncio.run(connector.fetch(path=tmp))
        assert docs == []
        results = asyncio.run(connector.extract_direct(path=tmp))
        assert results == []
