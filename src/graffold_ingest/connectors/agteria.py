"""Agteria/Atlas connector — ingest Atlas phase output markdown into the graph.

Atlas produces structured markdown files (phase-1-disease-map.md, phase-3-candidates.md,
phase-4-kill-report.md, phase-5-decision-memo.md, etc.) containing:
  - Markdown tables with target IDs, names, statuses, dispositions
  - Section headers with candidate names (## C1: Novel Compound X)
  - Disease/program context in the top-level heading
  - Cross-references between killed targets and re-proposed candidates

This connector reads those files and produces Documents that the LLM extractor
can further process, OR directly extracts entities via regex for speed (bypassing
LLM extraction when the markdown is already structured enough).

Usage:
    graffold-ingest pipeline --source agteria --path /path/to/programs/my-program/v1/

    # Or from Python:
    from graffold_ingest.connectors.agteria import AgteriaConnector
    connector = AgteriaConnector()
    docs = await connector.fetch(path="/path/to/atlas/programs/my-program/v1/")
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

from .base import Document, ExtractionResult

# Phase files in pipeline order
_PHASE_FILES = [
    "phase-1-disease-map.md",
    "phase-1a-anomaly-map.md",
    "phase-1b-bottleneck-consensus.md",
    "phase-2-failure-analysis.md",
    "phase-2b-competitive-landscape.md",
    "phase-3-candidates.md",
    "phase-3-vulcan.md",
    "phase-3c-literature-sweep.md",
    "phase-3d-feasibility-report.md",
    "phase-3b-survey-report.md",
    "phase-4-kill-report.md",
    "phase-4b-board-decision.md",
    "phase-5-coverage-map.md",
    "phase-5-evidence-register.md",
    "phase-5-decision-memo.md",
    "run-report.md",
]

# ─── Regex patterns for structured extraction ─────────────────────────────────

_TABLE_ROW = re.compile(r"^\|(.+)\|$")
_TARGET_ID = re.compile(r"(T-\d+|KILL-\d+)")
_CONFIDENCE = re.compile(r"P\s*=\s*([0-9.]+)")
_CANDIDATE_HEADER = re.compile(
    r"^#{2,4}\s*(?:Candidate\s+)?([A-Z]\d+|C\d+)[:\s\u2014\u2013-]+(.+?)(?:\(|$)",
    re.MULTILINE,
)
_DISEASE_HEADER = re.compile(r"^#\s+.+?[\u2014\u2013:-]\s*(.+?)$", re.MULTILINE)


class AgteriaConnector:
    """Read Atlas program output directories and produce Documents for extraction."""

    def name(self) -> str:
        return "agteria"

    async def fetch(
        self,
        *,
        path: str = "",
        phases: list[str] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        """Fetch Atlas phase output files as Documents.

        Args:
            path: Path to an Atlas program directory (e.g. programs/my-prog/v1/).
            phases: Optional list of specific phase files to include.
                    Default: all phase files found in the directory.

        Returns:
            List of Documents, one per phase file found.
        """
        if not path:
            return []

        program_dir = Path(path)
        if not program_dir.is_dir():
            return []

        # Determine program name from directory
        program_name = program_dir.name
        if program_name.startswith("v") and program_name[1:].isdigit():
            program_name = f"{program_dir.parent.name}/{program_name}"

        # Collect phase files
        target_files = phases or _PHASE_FILES
        docs: list[Document] = []

        for filename in target_files:
            filepath = program_dir / filename
            if not filepath.exists():
                continue

            content = filepath.read_text(encoding="utf-8", errors="replace")
            if not content.strip():
                continue

            doc_id = hashlib.sha256(
                f"{program_name}/{filename}".encode()
            ).hexdigest()[:16]

            # Derive phase key from filename
            phase_key = filepath.stem.replace("phase-", "").split("-")[0]

            docs.append(
                Document(
                    id=doc_id,
                    content=content,
                    source_url=str(filepath),
                    source_type="agteria",
                    title=f"{program_name} — {filename}",
                    metadata={
                        "program": program_name,
                        "phase_file": filename,
                        "phase_key": phase_key,
                    },
                )
            )

        return docs

    async def extract_direct(
        self,
        *,
        path: str = "",
        phases: list[str] | None = None,
    ) -> list[ExtractionResult]:
        """Bypass LLM extraction — directly parse structured markdown tables.

        Atlas markdown is already highly structured (tables, headers, IDs).
        This method regex-extracts entities and relationships without calling
        an LLM, which is faster and deterministic.

        For richer extraction (prose paragraphs, implicit relationships),
        use the normal pipeline: fetch() → chunk → LLM extract.
        """
        docs = await self.fetch(path=path, phases=phases)
        results: list[ExtractionResult] = []

        for doc in docs:
            program = doc.metadata.get("program", "unknown")
            phase_key = doc.metadata.get("phase_key", "unknown")
            nodes, edges = _extract_from_markdown(doc.content, phase_key, program)
            if nodes or edges:
                results.append(
                    ExtractionResult(
                        nodes=nodes,
                        edges=edges,
                        source_doc_id=doc.id,
                    )
                )

        return results


# ─── Structured extraction from Atlas markdown ────────────────────────────────


def _parse_md_tables(text: str) -> list[list[list[str]]]:
    """Extract all markdown tables as list of tables, each a list of rows."""
    tables: list[list[list[str]]] = []
    current: list[list[str]] = []
    for line in text.splitlines():
        m = _TABLE_ROW.match(line.strip())
        if m:
            cells = [c.strip() for c in m.group(1).split("|")]
            # Skip separator rows
            if all(re.match(r"^[-:]+$", c) for c in cells):
                continue
            current.append(cells)
        else:
            if current:
                tables.append(current)
                current = []
    if current:
        tables.append(current)
    return tables


def _extract_from_markdown(
    text: str, phase_key: str, program: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extract entities and relationships from Atlas phase markdown.

    Handles:
      - Target/kill tables (| Registry id | Target | Status |)
      - Candidate section headers (## C1: Name)
      - Disease from top-level heading
      - RE_PROPOSED_AS relationships from table columns
      - TARGETS_DISEASE relationships linking all entities to root disease
    """
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    # ─── Table extraction ──────────────────────────────────────────────
    tables = _parse_md_tables(text)

    for table in tables:
        if not table:
            continue
        header = [h.lower().strip() for h in table[0]]

        for row in table[1:]:
            if len(row) < len(header):
                row.extend([""] * (len(header) - len(row)))

            row_dict = dict(zip(header, row))

            target_id = None
            target_name = None
            target_type = "Target"

            # Look for ID columns
            for key in ("registry id", "portfolio id", "id", "target id"):
                if key in row_dict and row_dict[key].strip():
                    ids = _TARGET_ID.findall(row_dict[key])
                    if ids:
                        target_id = ids[0]
                        break

            # Look for name columns
            for key in ("target", "name", "candidate", "molecule", "killed molecule"):
                if key in row_dict and row_dict[key].strip():
                    target_name = re.sub(r"\*+", "", row_dict[key]).strip()
                    break

            if not target_name:
                continue

            # Entity type from context
            for key in ("type", "label", "category"):
                if key in row_dict:
                    t = row_dict[key].strip()
                    if t:
                        target_type = t
                        break

            entity_id = target_id or f"{phase_key}:{target_name.lower().replace(' ', '-')[:40]}"
            if entity_id in seen_ids:
                continue
            seen_ids.add(entity_id)

            entity: dict[str, Any] = {
                "id": entity_id,
                "name": target_name,
                "label": target_type,
                "description": row_dict.get(
                    "disposition", row_dict.get("forge disposition", "")
                ),
                "properties": {
                    "source_phase": phase_key,
                    "source_program": program,
                },
            }

            # Status
            for key in ("status", "disposition", "forge disposition", "kill reason"):
                if key in row_dict and row_dict[key].strip():
                    entity["properties"]["status"] = (
                        re.sub(r"\*+", "", row_dict[key]).strip()[:200]
                    )
                    break

            # Confidence
            conf_match = _CONFIDENCE.search(str(row_dict))
            if conf_match:
                entity["properties"]["confidence"] = float(conf_match.group(1))

            entities.append(entity)

            # Relationships from "re-proposed as" columns
            for key in ("target re-proposed as", "re-proposed as", "wound"):
                if key in row_dict and row_dict[key].strip():
                    related = re.sub(r"\*+", "", row_dict[key]).strip()
                    if related and len(related) > 2:
                        rel_id = f"{phase_key}:{related.lower().replace(' ', '-')[:40]}"
                        relationships.append({
                            "source_id": entity_id,
                            "target_id": rel_id,
                            "type": "RELATES_TO" if "wound" in key else "RE_PROPOSED_AS",
                            "description": related[:200],
                        })

    # ─── Candidate section headers ─────────────────────────────────────
    for m in _CANDIDATE_HEADER.finditer(text):
        cid = m.group(1).strip()
        cname = m.group(2).strip().rstrip("\u2014\u2013- ")
        eid = f"{phase_key}:candidate-{cid.lower()}"
        if eid not in seen_ids:
            seen_ids.add(eid)
            entities.append({
                "id": eid,
                "name": f"{cid}: {cname}",
                "label": "Candidate",
                "description": "",
                "properties": {"source_phase": phase_key, "source_program": program},
            })

    # ─── Disease root entity ───────────────────────────────────────────
    disease_match = _DISEASE_HEADER.search(text)
    if disease_match:
        disease_name = disease_match.group(1).strip()
        disease_id = f"disease:{disease_name.lower().replace(' ', '-')[:40]}"
        if disease_id not in seen_ids:
            seen_ids.add(disease_id)
            entities.append({
                "id": disease_id,
                "name": disease_name,
                "label": "Disease",
                "description": "",
                "properties": {"source_phase": phase_key, "source_program": program},
            })
            # Link all targets/candidates to disease
            for e in entities:
                if e["label"] in ("Target", "Candidate") and e["id"] != disease_id:
                    relationships.append({
                        "source_id": e["id"],
                        "target_id": disease_id,
                        "type": "TARGETS_DISEASE",
                    })

    return entities, relationships
