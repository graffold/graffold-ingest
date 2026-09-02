"""Graffold Audit — graph-powered quality checks for Atlas runs.

Uses the accumulated knowledge graph to verify, challenge, and enrich
Atlas pipeline output. Runs OUTSIDE Atlas — reads phase files from disk,
queries the graph, produces findings.

Audit domains powered by graph intelligence:

  1. OMISSION — targets in the graph's mechanism cluster that this run missed
  2. KILL_CONSISTENCY — targets simultaneously killed AND proposed
  3. EVIDENCE_COVERAGE — claims without VALIDATED_BY links in the graph
  4. VERSION_DRIFT — entity set divergence from prior runs
  5. NOVEL_CONNECTIONS — graph-predicted links the run didn't explore

Usage:
    from graffold_ingest.audit import run_audit
    findings = run_audit(
        program_dir="/path/to/atlas/programs/crypto-v11/v1/",
        parquet_root="~/.graffold/parquet/atlas-full/",
    )
    # findings is a list of AuditFinding with severity, domain, message

CLI:
    graffold-ingest audit ~/atlas/programs/crypto-v11/v1/ --graph ~/.graffold/parquet/atlas-full/
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


@dataclass
class AuditFinding:
    """A single audit finding."""

    domain: str  # OMISSION, KILL_CONSISTENCY, EVIDENCE_COVERAGE, etc.
    severity: str  # P0 (blocker), P1 (major), P2 (minor), P3 (info)
    message: str
    entity: str = ""  # affected entity name
    evidence: str = ""  # supporting evidence for the finding
    suggestion: str = ""  # what to do about it


@dataclass
class AuditReport:
    """Full audit report."""

    program_dir: str
    findings: list[AuditFinding] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    @property
    def blockers(self) -> list[AuditFinding]:
        return [f for f in self.findings if f.severity == "P0"]

    @property
    def verdict(self) -> str:
        if self.blockers:
            return "BLOCKED"
        return "CERTIFIED"

    def to_markdown(self) -> str:
        """Render as markdown for Atlas consumption."""
        lines = [
            "# Graffold Graph Audit Report",
            "",
            f"**Verdict: {self.verdict}**",
            f"**Findings:** {len(self.findings)} "
            f"({len(self.blockers)} blockers, "
            f"{len([f for f in self.findings if f.severity == 'P1'])} major, "
            f"{len([f for f in self.findings if f.severity == 'P2'])} minor)",
            "",
            "---",
            "",
        ]

        by_domain = defaultdict(list)
        for f in self.findings:
            by_domain[f.domain].append(f)

        for domain, findings in sorted(by_domain.items()):
            lines.append(f"## {domain}")
            lines.append("")
            for f in sorted(findings, key=lambda x: x.severity):
                icon = {"P0": "🔴", "P1": "🟠", "P2": "🟡", "P3": "🔵"}.get(
                    f.severity, "⚪"
                )
                lines.append(f"- {icon} **[{f.severity}]** {f.message}")
                if f.entity:
                    lines.append(f"  - Entity: `{f.entity}`")
                if f.evidence:
                    lines.append(f"  - Evidence: {f.evidence}")
                if f.suggestion:
                    lines.append(f"  - Suggestion: {f.suggestion}")
                lines.append("")
            lines.append("")

        # Stats
        lines.append("---")
        lines.append("")
        lines.append("## Audit Stats")
        lines.append("")
        for k, v in self.stats.items():
            lines.append(f"- **{k}**: {v}")

        return "\n".join(lines)


# ─── Graph loader ─────────────────────────────────────────────────────────────


class _GraphStore:
    """Loads entities and relationships from Parquet."""

    def __init__(self, parquet_dir: str | Path) -> None:
        self._dir = Path(parquet_dir).expanduser()
        self._entities: list[dict] = []
        self._relationships: list[dict] = []
        self._loaded = False

    def load(self) -> None:
        if self._loaded:
            return
        ent_path = self._dir / "entities.parquet"
        rel_path = self._dir / "relationships.parquet"
        if ent_path.exists():
            self._entities = pq.read_table(ent_path).to_pylist()
        if rel_path.exists():
            self._relationships = pq.read_table(rel_path).to_pylist()
        self._loaded = True

    @property
    def entities(self) -> list[dict]:
        self.load()
        return self._entities

    @property
    def relationships(self) -> list[dict]:
        self.load()
        return self._relationships


# ─── Audit domains ────────────────────────────────────────────────────────────


def _audit_omission(
    run_entities: list[dict], graph: _GraphStore
) -> list[AuditFinding]:
    """Find targets in the graph that this run should have considered but didn't.

    Logic: targets in the same mechanism cluster (share PART_OF relationships)
    that appear in the full graph but are absent from this run.
    """
    findings = []

    # What this run mentions
    run_names = {(e.get("name") or "").lower() for e in run_entities}

    # Graph targets
    [
        e for e in graph.entities if "target" in (e.get("type") or e.get("label") or "").lower()
    ]

    # Build mechanism clusters: mechanism_id -> set of target_ids
    mechanism_members: dict[str, set[str]] = defaultdict(set)
    target_mechanisms: dict[str, set[str]] = defaultdict(set)

    for r in graph.relationships:
        if r.get("type") == "PART_OF":
            src = r.get("source_id", "")
            tgt = r.get("target_id", "")
            mechanism_members[tgt].add(src)
            target_mechanisms[src].add(tgt)

    # Find mechanisms this run touches
    run_target_ids = set()
    for e in run_entities:
        if "target" in (e.get("type") or e.get("label") or "").lower():
            run_target_ids.add(e.get("id", ""))

    run_mechanisms = set()
    for tid in run_target_ids:
        run_mechanisms.update(target_mechanisms.get(tid, set()))

    # Find graph targets in same mechanisms but missing from this run
    id_to_name = {e["id"]: e.get("name", "") for e in graph.entities}
    omitted = set()

    for mech_id in run_mechanisms:
        for member_id in mechanism_members.get(mech_id, set()):
            member_name = id_to_name.get(member_id, "")
            if member_name and member_name.lower() not in run_names:
                omitted.add(member_name)

    for name in sorted(omitted)[:10]:
        findings.append(AuditFinding(
            domain="OMISSION",
            severity="P2",
            message=f"Target '{name}' is in the same mechanism cluster but not mentioned in this run",
            entity=name,
            evidence="Shares PART_OF relationship with targets in this run",
            suggestion="Consider whether this target was intentionally excluded or overlooked",
        ))

    return findings


def _audit_kill_consistency(
    run_entities: list[dict], graph: _GraphStore
) -> list[AuditFinding]:
    """Find targets that are both killed AND proposed in this run or across runs."""
    findings = []

    # Killed targets in graph
    killed_names: set[str] = set()
    kill_reasons: dict[str, str] = {}

    for e in graph.entities:
        eid = e.get("id", "")
        name = (e.get("name") or "").lower()
        desc = e.get("description", "") or ""
        if (
            eid.startswith("KILL-")
            or "killed" in desc.lower()
            or "not re-proposed" in desc.lower()
        ):
            killed_names.add(name)
            kill_reasons[name] = desc[:100]

    # Also check KILLED_BECAUSE relationships
    id_to_name = {e["id"]: (e.get("name") or "").lower() for e in graph.entities}
    for r in graph.relationships:
        if r.get("type") == "KILLED_BECAUSE":
            src_name = id_to_name.get(r.get("source_id", ""), "")
            if src_name:
                killed_names.add(src_name)
                kill_reasons[src_name] = r.get("description", "")[:100]

    # Check this run's proposed/active targets against kills
    for e in run_entities:
        etype = (e.get("type") or e.get("label") or "").lower()
        if "candidate" in etype or "target" in etype:
            name = (e.get("name") or "").lower()
            if name in killed_names and "kill" not in etype:
                reason = kill_reasons.get(name, "unknown")
                findings.append(AuditFinding(
                    domain="KILL_CONSISTENCY",
                    severity="P1",
                    message=f"Target '{e.get('name')}' is proposed but was previously killed",
                    entity=e.get("name", ""),
                    evidence=f"Kill reason: {reason}",
                    suggestion="Verify resurrection conditions are met or explicitly justify revival",
                ))

    return findings


def _audit_evidence_coverage(
    run_entities: list[dict], graph: _GraphStore
) -> list[AuditFinding]:
    """Find high-stakes claims without evidence backing in the graph."""
    findings = []

    # Targets/candidates in this run
    run_targets = [
        e for e in run_entities
        if "target" in (e.get("type") or e.get("label") or "").lower()
        or "candidate" in (e.get("type") or e.get("label") or "").lower()
    ]

    # Evidence nodes in graph
    evidence_linked: set[str] = set()
    for r in graph.relationships:
        if "VALIDATED" in (r.get("type") or "").upper():
            evidence_linked.add(r.get("source_id", ""))

    # Graph entity names that have evidence
    id_to_name = {e["id"]: (e.get("name") or "").lower() for e in graph.entities}
    names_with_evidence = {
        id_to_name.get(eid, "") for eid in evidence_linked
    }

    # Check run targets
    for e in run_targets:
        name = (e.get("name") or "").lower()
        if name and name not in names_with_evidence and len(name) > 3:
            findings.append(AuditFinding(
                domain="EVIDENCE_COVERAGE",
                severity="P2",
                message=f"Target '{e.get('name')}' has no validated evidence link in the knowledge graph",
                entity=e.get("name", ""),
                suggestion="Ensure load-bearing claims cite verified PMIDs",
            ))

    # Cap at 15 findings
    return findings[:15]


def _audit_version_drift(
    run_entities: list[dict], graph: _GraphStore
) -> list[AuditFinding]:
    """Detect significant divergence from prior runs.

    If this run introduces many targets not in the graph, or drops many that
    were previously active, flag it.
    """
    findings = []

    run_names = {
        (e.get("name") or "").lower()
        for e in run_entities
        if "target" in (e.get("type") or e.get("label") or "").lower()
    }

    graph_names = {
        (e.get("name") or "").lower()
        for e in graph.entities
        if "target" in (e.get("type") or e.get("label") or "").lower()
    }

    # New targets not in graph
    novel = run_names - graph_names
    if len(novel) > len(run_names) * 0.5 and len(novel) > 5:
        findings.append(AuditFinding(
            domain="VERSION_DRIFT",
            severity="P2",
            message=f"{len(novel)} targets in this run are absent from the knowledge graph "
                    f"({len(novel)}/{len(run_names)} = {100*len(novel)//max(len(run_names),1)}% novel)",
            evidence=f"Novel targets include: {', '.join(sorted(novel)[:5])}...",
            suggestion="Verify these are genuine discoveries, not hallucinated targets",
        ))

    # Graph targets dropped from this run (only flag if many disappeared)
    dropped = graph_names - run_names
    if len(dropped) > len(graph_names) * 0.8 and len(graph_names) > 20:
        findings.append(AuditFinding(
            domain="VERSION_DRIFT",
            severity="P3",
            message=f"This run mentions only {len(run_names)} of {len(graph_names)} "
                    f"targets in the knowledge graph ({len(dropped)} not mentioned)",
            suggestion="Expected for focused runs; flag only if broad coverage was intended",
        ))

    return findings


def _audit_novel_connections(
    run_entities: list[dict], graph: _GraphStore
) -> list[AuditFinding]:
    """Surface graph-predicted connections the run didn't explore.

    Finds entity pairs that share many neighbors in the graph but have no
    direct relationship — these are candidate novel insights.
    """
    findings = []

    # Build adjacency from graph
    neighbors: dict[str, set[str]] = defaultdict(set)
    for r in graph.relationships:
        src = r.get("source_id", "")
        tgt = r.get("target_id", "")
        if src and tgt:
            neighbors[src].add(tgt)
            neighbors[tgt].add(src)

    direct = {
        (r.get("source_id", ""), r.get("target_id", ""))
        for r in graph.relationships
    }
    direct |= {(b, a) for a, b in direct}

    id_to_name = {e["id"]: e.get("name", "") for e in graph.entities}
    {e["id"]: e.get("type", "") for e in graph.entities}

    # Only look at Target and Compound entities
    interesting_ids = [
        e["id"] for e in graph.entities
        if e.get("type", "") in ("Target", "Compound", "Mechanism")
    ]

    predictions = []
    for i, a in enumerate(interesting_ids):
        for b in interesting_ids[i + 1:]:
            if (a, b) in direct:
                continue
            shared = neighbors.get(a, set()) & neighbors.get(b, set())
            if len(shared) >= 3:
                predictions.append((
                    len(shared),
                    id_to_name.get(a, a),
                    id_to_name.get(b, b),
                    [id_to_name.get(s, s) for s in list(shared)[:3]],
                ))

    predictions.sort(reverse=True)
    for score, name_a, name_b, shared_names in predictions[:5]:
        findings.append(AuditFinding(
            domain="NOVEL_CONNECTIONS",
            severity="P3",
            message=f"Graph predicts connection between '{name_a}' and '{name_b}' "
                    f"({score} shared neighbors)",
            entity=f"{name_a} <-> {name_b}",
            evidence=f"Shared neighbors: {', '.join(shared_names)}",
            suggestion="Investigate whether this connection represents a novel hypothesis",
        ))

    return findings


# ─── Main entry point ─────────────────────────────────────────────────────────


def run_audit(
    program_dir: str | Path,
    parquet_root: str | Path = "~/.graffold/parquet/atlas-full",
    phases: list[str] | None = None,
) -> AuditReport:
    """Run graph-powered audit on an Atlas program directory.

    Args:
        program_dir: Path to Atlas program output (containing phase-*.md files).
        parquet_root: Path to Parquet knowledge graph.
        phases: Specific phase files to audit (default: all found).

    Returns:
        AuditReport with findings, verdict, and markdown output.
    """
    import asyncio

    from .connectors.agteria import AgteriaConnector

    program_dir = Path(program_dir).expanduser()
    graph = _GraphStore(parquet_root)

    # Extract entities from this run
    connector = AgteriaConnector()
    run_results = asyncio.run(
        connector.extract_direct(path=str(program_dir), phases=phases)
    )
    run_entities = [n for r in run_results for n in r.nodes]

    logger.info(
        "Auditing %s: %d run entities against %d graph entities",
        program_dir.name, len(run_entities), len(graph.entities),
    )

    # Run all domains
    all_findings: list[AuditFinding] = []
    all_findings.extend(_audit_omission(run_entities, graph))
    all_findings.extend(_audit_kill_consistency(run_entities, graph))
    all_findings.extend(_audit_evidence_coverage(run_entities, graph))
    all_findings.extend(_audit_version_drift(run_entities, graph))
    all_findings.extend(_audit_novel_connections(run_entities, graph))

    report = AuditReport(
        program_dir=str(program_dir),
        findings=all_findings,
        stats={
            "run_entities": len(run_entities),
            "graph_entities": len(graph.entities),
            "graph_relationships": len(graph.relationships),
            "domains_checked": 5,
        },
    )

    logger.info(
        "Audit complete: %s (%d findings, %d blockers)",
        report.verdict, len(report.findings), len(report.blockers),
    )

    return report
