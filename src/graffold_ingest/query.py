"""Graffold Query Engine — cross-run intelligence from accumulated knowledge graphs.

The value layer: answers questions Atlas cannot answer from a single run.
Operates over Parquet files — no Neo4j, no server, no infrastructure.

Key capabilities:
  1. prior_knowledge() — "What do we already know about disease X?"
     → Generates a context document Atlas can consume at startup
  2. cross_run_kills() — "Which targets were killed across ALL programs, and why?"
     → Propagates decisions automatically
  3. target_trajectory() — "Show me the full history of target X across all runs"
     → Kill, revival, re-proposal, evidence accumulation
  4. novel_predictions() — "What connections exist that no single run discovered?"
     → Graph-structure-based inference (shared neighbors, path patterns)
  5. evidence_gaps() — "Which high-confidence targets lack validated evidence?"
     → Prioritizes what to investigate next

Usage:
    from graffold_ingest.query import QueryEngine
    engine = QueryEngine("~/.graffold/parquet/")
    
    # Generate startup context for a new Atlas run
    context = engine.prior_knowledge("cryptosporidiosis")
    Path("prior-knowledge.md").write_text(context)
    
    # Find targets killed everywhere
    kills = engine.cross_run_kills("cryptosporidiosis")
    
    # Track a target across runs
    history = engine.target_trajectory("CpTrxR")
"""

from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class QueryEngine:
    """Cross-run query engine over accumulated Parquet knowledge graphs.

    Reads all program-level Parquet stores under a root directory.
    No server, no database — just files.
    """

    def __init__(self, parquet_root: str | Path = "~/.graffold/parquet") -> None:
        self._root = Path(parquet_root).expanduser()
        self._entities: list[dict] = []
        self._relationships: list[dict] = []
        self._programs: list[str] = []
        self._loaded = False

    def _load(self) -> None:
        """Load all Parquet files from all programs."""
        if self._loaded:
            return

        for program_dir in sorted(self._root.iterdir()):
            if not program_dir.is_dir():
                continue
            ent_path = program_dir / "entities.parquet"
            rel_path = program_dir / "relationships.parquet"
            if not ent_path.exists():
                continue

            program = program_dir.name
            self._programs.append(program)

            entities = pq.read_table(ent_path).to_pylist()
            for e in entities:
                e["_program"] = program
            self._entities.extend(entities)

            if rel_path.exists():
                rels = pq.read_table(rel_path).to_pylist()
                for r in rels:
                    r["_program"] = program
                self._relationships.extend(rels)

        self._loaded = True
        logger.info(
            "Loaded %d entities, %d relationships from %d programs",
            len(self._entities), len(self._relationships), len(self._programs),
        )

    # ─── 1. Prior Knowledge Generation ────────────────────────────────────────

    def prior_knowledge(self, disease: str, *, max_targets: int = 30) -> str:
        """Generate a prior-knowledge.md document for Atlas startup.

        This is the killer feature: before Atlas starts a new run, it gets
        everything the graph already knows about this disease — targets explored,
        decisions made, evidence accumulated, gaps identified.

        Returns markdown that Atlas can consume directly as additional context.
        """
        self._load()
        disease_lower = disease.lower()

        # Find all entities related to this disease
        targets = []
        compounds = []
        mechanisms = []
        evidence = []
        decisions = []

        for e in self._entities:
            name_lower = (e.get("name") or "").lower()
            desc_lower = (e.get("description") or "").lower()
            etype = (e.get("type") or "").lower()

            # Match by disease mention in name, description, or source_doc
            if disease_lower not in name_lower and disease_lower not in desc_lower:
                source = (e.get("source_doc_id") or "").lower()
                if disease_lower not in source:
                    continue

            if "target" in etype:
                targets.append(e)
            elif "compound" in etype or "drug" in etype:
                compounds.append(e)
            elif "mechanism" in etype:
                mechanisms.append(e)
            elif "evidence" in etype:
                evidence.append(e)
            elif "decision" in etype:
                decisions.append(e)

        # Find relationships involving these entities
        entity_ids = {e["id"] for e in targets + compounds + mechanisms}
        relevant_rels = [
            r for r in self._relationships
            if r.get("source_id") in entity_ids or r.get("target_id") in entity_ids
        ]

        # Build the context document
        lines = [
            f"# Prior Knowledge: {disease.title()}",
            "",
            f"*Auto-generated from {len(self._programs)} accumulated program runs.*",
            (
                f"*{len(targets)} targets, {len(compounds)} compounds, "
                f"{len(relevant_rels)} relationships in the knowledge graph.*"
            ),
            "",
            "---",
            "",
        ]

        # Targets section
        if targets:
            lines.append("## Known Targets")
            lines.append("")
            lines.append("| Target | Status | Programs | Key Evidence |")
            lines.append("|--------|--------|----------|--------------|")
            seen = set()
            for t in sorted(targets, key=lambda x: x.get("name", ""))[:max_targets]:
                name = t.get("name", "?")
                if name.lower() in seen:
                    continue
                seen.add(name.lower())
                status = t.get("description", "")[:60] or "Active"
                program = t.get("_program", "?")
                # Find evidence links
                t_evidence = [
                    r for r in relevant_rels
                    if r.get("source_id") == t["id"] and "VALIDATED" in (r.get("type") or "")
                ]
                ev_str = f"{len(t_evidence)} citations" if t_evidence else "—"
                lines.append(f"| {name} | {status} | {program} | {ev_str} |")
            lines.append("")

        # Kill decisions
        kill_rels = [r for r in relevant_rels if "KILL" in (r.get("type") or "").upper()]
        if kill_rels or decisions:
            lines.append("## Prior Kill Decisions")
            lines.append("")
            lines.append("Do NOT re-propose these without meeting resurrection conditions:")
            lines.append("")
            for r in kill_rels:
                src = r.get("source_id", "?")
                desc = r.get("description", "")[:100]
                lines.append(f"- **{src}**: {desc}")
            for d in decisions:
                lines.append(f"- **{d.get('name', '?')}**: {d.get('description', '')[:100]}")
            lines.append("")

        # Mechanisms
        if mechanisms:
            lines.append("## Known Mechanisms & Pathways")
            lines.append("")
            for m in mechanisms[:15]:
                lines.append(f"- **{m.get('name', '?')}**: {m.get('description', '')[:80]}")
            lines.append("")

        # Compounds explored
        if compounds:
            lines.append("## Compounds Explored")
            lines.append("")
            for c in compounds[:15]:
                lines.append(f"- **{c.get('name', '?')}**: {c.get('description', '')[:80]}")
            lines.append("")

        # Relationship summary
        rel_types = defaultdict(int)
        for r in relevant_rels:
            rel_types[r.get("type", "?")] += 1
        if rel_types:
            lines.append("## Relationship Summary")
            lines.append("")
            lines.append("| Type | Count |")
            lines.append("|------|-------|")
            for t, c in sorted(rel_types.items(), key=lambda x: -x[1])[:10]:
                lines.append(f"| {t} | {c} |")
            lines.append("")

        # Gaps
        lines.append("## Evidence Gaps")
        lines.append("")
        lines.append("Targets mentioned but lacking VALIDATED_BY relationships:")
        lines.append("")
        validated_targets = {
            r["source_id"] for r in relevant_rels if "VALIDATED" in (r.get("type") or "")
        }
        unvalidated = [
            t for t in targets
            if t["id"] not in validated_targets and t.get("name")
        ]
        for t in unvalidated[:10]:
            lines.append(f"- {t.get('name', '?')} (needs evidence)")
        lines.append("")

        return "\n".join(lines)

    # ─── 2. Cross-Run Kill Propagation ────────────────────────────────────────

    def cross_run_kills(self, disease: str = "") -> list[dict[str, Any]]:
        """Find all killed targets, with reasons, across all programs.

        Returns list of {target, reason, program, kill_id} sorted by frequency.
        Targets killed in multiple programs are highest priority "don't re-propose."
        """
        self._load()
        disease_lower = disease.lower() if disease else ""

        kills: dict[str, list[dict]] = defaultdict(list)

        for e in self._entities:
            name = e.get("name", "")
            eid = e.get("id", "")
            desc = e.get("description", "")

            # Identify kills by ID pattern or description
            is_kill = (
                eid.startswith("KILL-")
                or "killed" in desc.lower()
                or "not re-proposed" in desc.lower()
                or "not revisited" in desc.lower()
            )
            if not is_kill:
                continue

            if disease_lower:
                source = (e.get("source_doc_id") or "").lower()
                if disease_lower not in source and disease_lower not in name.lower():
                    continue

            kills[name.lower()].append({
                "target": name,
                "id": eid,
                "reason": desc[:200],
                "program": e.get("_program", "?"),
            })

        # Sort by number of programs that killed it
        result = []
        for name, instances in sorted(kills.items(), key=lambda x: -len(x[1])):
            result.append({
                "target": instances[0]["target"],
                "kill_count": len(instances),
                "programs": list({i["program"] for i in instances}),
                "reasons": [i["reason"] for i in instances if i["reason"]],
                "ids": [i["id"] for i in instances],
            })

        return result

    # ─── 3. Target Trajectory ─────────────────────────────────────────────────

    def target_trajectory(self, target_name: str) -> dict[str, Any]:
        """Full history of a target across all runs.

        Returns timeline of mentions, status changes, evidence, and decisions.
        """
        self._load()
        target_lower = target_name.lower()

        mentions = []
        for e in self._entities:
            if target_lower in (e.get("name") or "").lower():
                mentions.append({
                    "name": e.get("name"),
                    "type": e.get("type"),
                    "status": e.get("description", "")[:100],
                    "program": e.get("_program"),
                    "source": e.get("source_doc_id"),
                    "id": e.get("id"),
                })

        # Find all relationships involving this target
        target_ids = {m["id"] for m in mentions}
        relationships = [
            r for r in self._relationships
            if r.get("source_id") in target_ids or r.get("target_id") in target_ids
        ]

        return {
            "target": target_name,
            "mention_count": len(mentions),
            "programs": list({m["program"] for m in mentions}),
            "mentions": mentions,
            "relationships": relationships,
            "status_history": [
                {"program": m["program"], "status": m["status"]}
                for m in mentions if m["status"]
            ],
        }

    # ─── 4. Novel Predictions (graph-structure) ───────────────────────────────

    def novel_predictions(self, min_shared_neighbors: int = 2) -> list[dict[str, Any]]:
        """Find entity pairs that share neighbors but have no direct relationship.

        These are candidate novel predictions: "A and B are both connected to C and D,
        but A and B are never directly linked — maybe they should be."

        This is the value GraphRAG cannot provide — structural inference.
        """
        self._load()

        # Build adjacency: entity_id -> set of connected entity_ids
        neighbors: dict[str, set[str]] = defaultdict(set)
        for r in self._relationships:
            src = r.get("source_id", "")
            tgt = r.get("target_id", "")
            if src and tgt:
                neighbors[src].add(tgt)
                neighbors[tgt].add(src)

        # Find existing direct edges
        direct_edges: set[tuple[str, str]] = set()
        for r in self._relationships:
            src = r.get("source_id", "")
            tgt = r.get("target_id", "")
            direct_edges.add((src, tgt))
            direct_edges.add((tgt, src))

        # Find pairs with shared neighbors but no direct link
        id_to_name: dict[str, str] = {
            e["id"]: e.get("name", e["id"]) for e in self._entities
        }
        id_to_type: dict[str, str] = {
            e["id"]: e.get("type", "?") for e in self._entities
        }

        predictions: list[dict[str, Any]] = []
        checked: set[tuple[str, str]] = set()

        entity_ids = list(neighbors.keys())
        for i, a in enumerate(entity_ids):
            for b in entity_ids[i + 1:]:
                if (a, b) in checked or (a, b) in direct_edges:
                    continue
                checked.add((a, b))

                shared = neighbors[a] & neighbors[b]
                if len(shared) >= min_shared_neighbors:
                    predictions.append({
                        "entity_a": id_to_name.get(a, a),
                        "entity_b": id_to_name.get(b, b),
                        "type_a": id_to_type.get(a, "?"),
                        "type_b": id_to_type.get(b, "?"),
                        "shared_neighbors": len(shared),
                        "shared_names": [id_to_name.get(s, s) for s in list(shared)[:5]],
                    })

        # Sort by shared neighbor count
        predictions.sort(key=lambda x: -x["shared_neighbors"])
        return predictions[:50]

    # ─── 5. Evidence Gaps ─────────────────────────────────────────────────────

    def evidence_gaps(self) -> list[dict[str, Any]]:
        """Find high-value targets that lack validated evidence.

        Targets mentioned in multiple programs but with no VALIDATED_BY relationship
        are the highest-priority gaps — things everyone assumes are real but nobody proved.
        """
        self._load()

        # Count target mentions
        target_mentions: dict[str, dict] = {}
        for e in self._entities:
            if "target" not in (e.get("type") or "").lower():
                continue
            name = (e.get("name") or "").lower()
            if not name:
                continue
            if name not in target_mentions:
                target_mentions[name] = {
                    "name": e.get("name"),
                    "programs": set(),
                    "ids": set(),
                }
            target_mentions[name]["programs"].add(e.get("_program", "?"))
            target_mentions[name]["ids"].add(e["id"])

        # Find which have VALIDATED_BY
        validated_ids: set[str] = set()
        for r in self._relationships:
            if "VALIDATED" in (r.get("type") or "").upper():
                validated_ids.add(r.get("source_id", ""))

        # Gaps: mentioned but not validated
        gaps = []
        for name, info in target_mentions.items():
            has_evidence = bool(info["ids"] & validated_ids)
            if not has_evidence:
                gaps.append({
                    "target": info["name"],
                    "programs": list(info["programs"]),
                    "mention_count": len(info["programs"]),
                })

        gaps.sort(key=lambda x: -x["mention_count"])
        return gaps[:30]

    # ─── Stats ────────────────────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        """Summary statistics of the accumulated graph."""
        self._load()
        types = defaultdict(int)
        for e in self._entities:
            types[e.get("type", "?")] += 1
        rel_types = defaultdict(int)
        for r in self._relationships:
            rel_types[r.get("type", "?")] += 1

        return {
            "programs": len(self._programs),
            "total_entities": len(self._entities),
            "total_relationships": len(self._relationships),
            "entity_types": dict(sorted(types.items(), key=lambda x: -x[1])),
            "relationship_types": dict(sorted(rel_types.items(), key=lambda x: -x[1])),
        }
