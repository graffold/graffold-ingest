"""Publish extracted entities to Parquet files in GraphRAG-compatible schema.

Stores graph data as columnar Parquet tables:
- entities.parquet
- relationships.parquet
- communities.parquet
- text_units.parquet
- documents.parquet

Each write appends to existing files. The store is append-only; use
read_parquet_graph(latest=True) to collapse to the most recent version per ID.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..connectors.base import ExtractionResult

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path.home() / ".graffold" / "parquet"

# ─── Schemas ───────────────────────────────────────────────────────────────────

ENTITIES_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("type", pa.string()),
        pa.field("description", pa.string()),
        pa.field("source_doc_id", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("community_id", pa.string()),
        pa.field("level", pa.int32()),
        pa.field("ingested_at", pa.int64()),
        pa.field("extraction_method", pa.string()),
    ]
)

RELATIONSHIPS_SCHEMA = pa.schema(
    [
        pa.field("source_id", pa.string()),
        pa.field("target_id", pa.string()),
        pa.field("type", pa.string()),
        pa.field("weight", pa.float64()),
        pa.field("description", pa.string()),
        pa.field("source_doc_id", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("ingested_at", pa.int64()),
    ]
)

COMMUNITIES_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("level", pa.int32()),
        pa.field("title", pa.string()),
        pa.field("summary", pa.string()),
        pa.field("parent_id", pa.string()),
        pa.field("size", pa.int32()),
    ]
)

TEXT_UNITS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("text", pa.string()),
        pa.field("source_doc_id", pa.string()),
        pa.field("entity_ids", pa.string()),
        pa.field("relationship_ids", pa.string()),
    ]
)

DOCUMENTS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("title", pa.string()),
        pa.field("source_url", pa.string()),
        pa.field("source_type", pa.string()),
        pa.field("ingested_at", pa.int64()),
    ]
)


# ─── Internal helpers ──────────────────────────────────────────────────────────


def _ensure_dir(output_dir: Path) -> None:
    """Create output directory if it doesn't exist."""
    output_dir.mkdir(parents=True, exist_ok=True)


def _read_existing(path: Path, schema: pa.Schema) -> pa.Table:
    """Read existing Parquet file or return empty table with schema."""
    if path.exists():
        return pq.read_table(path, schema=schema)
    return pa.table({f.name: pa.array([], type=f.type) for f in schema})


def _dedup_and_write(
    path: Path,
    new_table: pa.Table,
    schema: pa.Schema,
    id_column: str = "id",
) -> int:
    """Read existing file, deduplicate by ID, concat, and write back.

    New records overwrite existing records with the same ID (MERGE semantics).
    Returns the number of new records written.
    """
    existing = _read_existing(path, schema)
    new_ids = set(new_table.column(id_column).to_pylist())

    if existing.num_rows > 0:
        # Filter out existing records whose ID appears in new data
        mask = [
            eid not in new_ids
            for eid in existing.column(id_column).to_pylist()
        ]
        existing = existing.filter(mask)

    merged = pa.concat_tables([existing, new_table], promote_options="default")
    pq.write_table(merged, path)
    return new_table.num_rows


def _append_write(
    path: Path,
    new_table: pa.Table,
    schema: pa.Schema,
) -> int:
    """Append to existing file without dedup (for tables without unique IDs)."""
    existing = _read_existing(path, schema)
    merged = pa.concat_tables([existing, new_table], promote_options="default")
    pq.write_table(merged, path)
    return new_table.num_rows


# ─── Public API ────────────────────────────────────────────────────────────────


async def publish_to_parquet(
    results: list[ExtractionResult],
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    documents: list[dict] | None = None,
    text_units: list[dict] | None = None,
    communities: list[dict] | None = None,
    run_id: str = "",
) -> dict[str, int]:
    """Write extraction results to Parquet files (append-only).

    Always appends — never overwrites. Every ingest is a timestamped
    snapshot with run_id for reproducibility.

    Args:
        results: Extraction results containing nodes and edges.
        output_dir: Directory for Parquet output files.
        documents: Optional document metadata records.
        text_units: Optional text unit (chunk) records.
        communities: Optional community records.

    Returns:
        Dict with counts: entities_written, relationships_written, etc.
    """
    output_dir = Path(output_dir)
    _ensure_dir(output_dir)
    ingested_at = int(time.time() * 1000)
    if not run_id:
        import uuid as _uuid
        run_id = str(_uuid.uuid4())[:8]

    counts: dict[str, int] = {}

    # ─── Entities ──────────────────────────────────────────────────────
    entity_records: list[dict[str, Any]] = []
    for result in results:
        for node in result.nodes:
            entity_records.append(
                {
                    "id": node.get("id", ""),
                    "name": node.get("name", node.get("id", "")),
                    "type": node.get("label", node.get("type", "Entity")),
                    "description": node.get("description", ""),
                    "source_doc_id": result.source_doc_id,
                    "run_id": run_id,
                    "community_id": node.get("community_id", ""),
                    "level": node.get("level", 0),
                    "ingested_at": ingested_at,
                    "extraction_method": node.get("extraction_method", "llm"),
                }
            )

    if entity_records:
        entities_table = pa.Table.from_pylist(
            entity_records, schema=ENTITIES_SCHEMA
        )
        counts["entities_written"] = _append_write(
            output_dir / "entities.parquet",
            entities_table,
            ENTITIES_SCHEMA,
        )
    else:
        counts["entities_written"] = 0

    # ─── Relationships ─────────────────────────────────────────────────
    rel_records: list[dict[str, Any]] = []
    for result in results:
        for edge in result.edges:
            rel_records.append(
                {
                    "source_id": edge.get("source_id", ""),
                    "target_id": edge.get("target_id", ""),
                    "type": edge.get("type", "RELATED_TO"),
                    "weight": float(edge.get("weight", 1.0)),
                    "description": edge.get(
                        "description", edge.get("source_sentence", "")
                    ),
                    "source_doc_id": result.source_doc_id,
                    "run_id": run_id,
                    "ingested_at": ingested_at,
                }
            )

    if rel_records:
        rels_table = pa.Table.from_pylist(rel_records, schema=RELATIONSHIPS_SCHEMA)
        # Dedup relationships by composite key: source_id + target_id + type
        # Use source_id as the dedup column isn't ideal, so we append instead
        counts["relationships_written"] = _append_write(
            output_dir / "relationships.parquet",
            rels_table,
            RELATIONSHIPS_SCHEMA,
        )
    else:
        counts["relationships_written"] = 0

    # ─── Communities ───────────────────────────────────────────────────
    if communities:
        community_records = [
            {
                "id": c.get("community_id", c.get("id", "")),
                "level": c.get("level", 0),
                "title": c.get("title", ""),
                "summary": c.get("summary", ""),
                "parent_id": c.get("parent_id", "") or "",
                "size": c.get("member_count", c.get("size", 0)),
            }
            for c in communities
        ]
        comm_table = pa.Table.from_pylist(community_records, schema=COMMUNITIES_SCHEMA)
        counts["communities_written"] = _append_write(
            output_dir / "communities.parquet",
            comm_table,
            COMMUNITIES_SCHEMA,
        )
    else:
        counts["communities_written"] = 0

    # ─── Text Units ────────────────────────────────────────────────────
    if text_units:
        tu_records = [
            {
                "id": tu.get("id", ""),
                "text": tu.get("text", ""),
                "source_doc_id": tu.get("source_doc_id", ""),
                "entity_ids": json.dumps(tu.get("entity_ids", [])),
                "relationship_ids": json.dumps(tu.get("relationship_ids", [])),
            }
            for tu in text_units
        ]
        tu_table = pa.Table.from_pylist(tu_records, schema=TEXT_UNITS_SCHEMA)
        counts["text_units_written"] = _append_write(
            output_dir / "text_units.parquet",
            tu_table,
            TEXT_UNITS_SCHEMA,
        )
    else:
        counts["text_units_written"] = 0

    # ─── Documents ─────────────────────────────────────────────────────
    if documents:
        doc_records = [
            {
                "id": d.get("id", ""),
                "title": d.get("title", ""),
                "source_url": d.get("source_url", ""),
                "source_type": d.get("source_type", ""),
                "ingested_at": d.get("ingested_at", ingested_at),
            }
            for d in documents
        ]
        doc_table = pa.Table.from_pylist(doc_records, schema=DOCUMENTS_SCHEMA)
        counts["documents_written"] = _append_write(
            output_dir / "documents.parquet",
            doc_table,
            DOCUMENTS_SCHEMA,
        )
    else:
        counts["documents_written"] = 0

    logger.info(
        "Parquet publish: %d entities, %d relationships, %d communities",
        counts["entities_written"],
        counts["relationships_written"],
        counts["communities_written"],
    )
    return counts


def read_parquet_graph(
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    latest: bool = False,
) -> tuple[list[dict], list[dict]]:
    """Read entities and relationships from Parquet.

    Args:
        output_dir: Directory containing Parquet files.
        latest: If True, deduplicate by ID keeping the most recent
            row per entity (by ingested_at). The store is append-only,
            so the raw file may hold multiple versions of the same ID.

    Returns:
        Tuple of (nodes, edges) as lists of dicts.
    """
    output_dir = Path(output_dir)
    nodes: list[dict] = []
    edges: list[dict] = []

    entities_path = output_dir / "entities.parquet"
    if entities_path.exists():
        table = pq.read_table(entities_path, schema=ENTITIES_SCHEMA)
        nodes = table.to_pylist()

    rels_path = output_dir / "relationships.parquet"
    if rels_path.exists():
        table = pq.read_table(rels_path, schema=RELATIONSHIPS_SCHEMA)
        edges = table.to_pylist()

    if latest:
        nodes = _latest_by_id(nodes, "id")
        edges = _latest_by_edge(edges)

    return nodes, edges


def _latest_by_id(rows: list[dict], id_col: str) -> list[dict]:
    """Keep the most recent row per ID (by ingested_at)."""
    seen: dict[str, dict] = {}
    for r in rows:
        rid = r.get(id_col, "")
        if rid not in seen or r.get("ingested_at", 0) >= seen[rid].get("ingested_at", 0):
            seen[rid] = r
    return list(seen.values())


def _latest_by_edge(rows: list[dict]) -> list[dict]:
    """Keep the most recent row per (source_id, target_id, type)."""
    seen: dict[tuple, dict] = {}
    for r in rows:
        key = (r.get("source_id", ""), r.get("target_id", ""), r.get("type", ""))
        if key not in seen or r.get("ingested_at", 0) >= seen[key].get("ingested_at", 0):
            seen[key] = r
    return list(seen.values())


def parquet_stats(
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, int]:
    """Get counts of entities, relationships, communities in Parquet store.

    Args:
        output_dir: Directory containing Parquet files.

    Returns:
        Dict with row counts for each table that exists.
    """
    output_dir = Path(output_dir)
    stats: dict[str, int] = {}

    for name in ("entities", "relationships", "communities", "text_units", "documents"):
        path = output_dir / f"{name}.parquet"
        if path.exists():
            meta = pq.read_metadata(path)
            stats[name] = meta.num_rows
        else:
            stats[name] = 0

    return stats
