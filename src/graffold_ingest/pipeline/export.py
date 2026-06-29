"""Knowledge Graph export — dump your graph to DuckDB, Parquet, JSONL, or TSV.

Usage:
    graffold-ingest export --format duckdb --output my_graph.duckdb
    graffold-ingest export --format parquet --output ./export/
    graffold-ingest export --format jsonl --output graph.jsonl
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


async def export_graph(
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "",
    password: str = "",
    output: str = "graph_export.duckdb",
    format: str = "duckdb",
    limit: int = 0,
) -> dict[str, Any]:
    """Export the knowledge graph to a portable format.

    Args:
        database_uri: Bolt URI for the graph database.
        database_name: Database name.
        username/password: Auth credentials.
        output: Output file or directory path.
        format: Export format — "duckdb", "parquet", "jsonl", "tsv".
        limit: Max nodes to export (0 = all).

    Returns:
        Dict with export stats.
    """
    t0 = time.time()

    # Load all nodes and edges from the graph
    nodes, edges = await _load_graph(database_uri, database_name, username, password, limit)

    if format == "duckdb":
        stats = _export_duckdb(nodes, edges, output)
    elif format == "parquet":
        stats = _export_parquet(nodes, edges, output)
    elif format == "jsonl":
        stats = _export_jsonl(nodes, edges, output)
    elif format == "tsv":
        stats = _export_tsv(nodes, edges, output)
    else:
        raise ValueError(f"Unknown format: {format}. Use: duckdb, parquet, jsonl, tsv")

    stats["elapsed_seconds"] = round(time.time() - t0, 2)
    logger.info("Exported %d nodes, %d edges to %s (%s)", stats["nodes"], stats["edges"], output, format)
    return stats


async def _load_graph(
    uri: str, database: str, user: str, password: str, limit: int
) -> tuple[list[dict], list[dict]]:
    """Load nodes and edges from the graph database."""
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(uri, auth=(user, password) if user else None)

    limit_clause = f"LIMIT {limit}" if limit else ""

    nodes = []
    with driver.session(database=database if database else None) as session:
        result = session.run(
            f"MATCH (n) RETURN n.id AS id, n.name AS name, labels(n) AS labels, "
            f"properties(n) AS props {limit_clause}"
        )
        for record in result:
            node = {
                "id": record["id"] or "",
                "name": record["name"] or "",
                "labels": record["labels"] or [],
                **(record["props"] or {}),
            }
            # Flatten labels to a string
            node["_labels"] = "|".join(record["labels"] or [])
            nodes.append(node)

    edges = []
    with driver.session(database=database if database else None) as session:
        result = session.run(
            f"MATCH (a)-[r]->(b) RETURN a.id AS src, b.id AS tgt, type(r) AS rel, "
            f"properties(r) AS props {limit_clause}"
        )
        for record in result:
            edge = {
                "source": record["src"] or "",
                "target": record["tgt"] or "",
                "relation": record["rel"] or "",
                **(record["props"] or {}),
            }
            edges.append(edge)

    driver.close()
    return nodes, edges


def _export_duckdb(nodes: list[dict], edges: list[dict], output: str) -> dict[str, int]:
    """Export to a DuckDB file with nodes and edges tables."""
    try:
        import duckdb
    except ImportError:
        raise ImportError("Install duckdb: pip install duckdb")

    import pandas as pd

    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)

    nodes_df = pd.DataFrame(nodes)
    edges_df = pd.DataFrame(edges)

    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE nodes AS SELECT * FROM nodes_df")
    con.execute("CREATE TABLE edges AS SELECT * FROM edges_df")

    # Add useful indexes
    con.execute("CREATE INDEX idx_nodes_id ON nodes(id)")
    con.execute("CREATE INDEX idx_edges_src ON edges(source)")
    con.execute("CREATE INDEX idx_edges_tgt ON edges(target)")
    con.execute("CREATE INDEX idx_edges_rel ON edges(relation)")

    con.close()
    return {"nodes": len(nodes), "edges": len(edges), "output": str(path), "format": "duckdb"}


def _export_parquet(nodes: list[dict], edges: list[dict], output: str) -> dict[str, int]:
    """Export to Parquet files (nodes.parquet + edges.parquet)."""
    import pandas as pd

    out_dir = Path(output)
    out_dir.mkdir(parents=True, exist_ok=True)

    nodes_df = pd.DataFrame(nodes)
    edges_df = pd.DataFrame(edges)

    nodes_df.to_parquet(out_dir / "nodes.parquet", index=False)
    edges_df.to_parquet(out_dir / "edges.parquet", index=False)

    return {"nodes": len(nodes), "edges": len(edges), "output": str(out_dir), "format": "parquet"}


def _export_jsonl(nodes: list[dict], edges: list[dict], output: str) -> dict[str, int]:
    """Export to JSONL (KGX-compatible: nodes and edges interleaved)."""
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        for node in nodes:
            f.write(json.dumps({"type": "node", **node}) + "\n")
        for edge in edges:
            f.write(json.dumps({"type": "edge", **edge}) + "\n")

    return {"nodes": len(nodes), "edges": len(edges), "output": str(path), "format": "jsonl"}


def _export_tsv(nodes: list[dict], edges: list[dict], output: str) -> dict[str, int]:
    """Export to TSV files (nodes.tsv + edges.tsv) — KGX-compatible."""
    out_dir = Path(output)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Nodes TSV
    with open(out_dir / "nodes.tsv", "w") as f:
        if nodes:
            cols = ["id", "name", "_labels"]
            f.write("\t".join(cols) + "\n")
            for node in nodes:
                f.write("\t".join(str(node.get(c, "")) for c in cols) + "\n")

    # Edges TSV
    with open(out_dir / "edges.tsv", "w") as f:
        if edges:
            cols = ["source", "relation", "target"]
            f.write("\t".join(cols) + "\n")
            for edge in edges:
                f.write("\t".join(str(edge.get(c, "")) for c in cols) + "\n")

    return {"nodes": len(nodes), "edges": len(edges), "output": str(out_dir), "format": "tsv"}
