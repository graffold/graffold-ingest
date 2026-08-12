"""Dual-write publisher — writes to Neo4j and/or Parquet backends.

Orchestrates publishing to both backends with independent failure handling.
Either backend can be disabled without affecting the other.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..connectors.base import ExtractionResult
from .publish import publish_to_graph
from .publish_parquet import DEFAULT_OUTPUT_DIR, publish_to_parquet

logger = logging.getLogger(__name__)


async def publish_dual(
    results: list[ExtractionResult],
    *,
    # Neo4j options
    neo4j_enabled: bool = True,
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "neo4j",
    password: str = "",
    # Parquet options
    parquet_enabled: bool = True,
    parquet_dir: str | Path = DEFAULT_OUTPUT_DIR,
    # Extras
    documents: list[dict] | None = None,
    text_units: list[dict] | None = None,
    communities: list[dict] | None = None,
) -> dict[str, Any]:
    """Publish to both Neo4j and Parquet (dual-write mode).

    Either backend can be disabled independently.
    Failures in one backend don't block the other.

    Args:
        results: Extraction results to publish.
        neo4j_enabled: Whether to write to Neo4j.
        database_uri: Neo4j connection URI.
        database_name: Neo4j database name.
        username: Neo4j username.
        password: Neo4j password.
        parquet_enabled: Whether to write to Parquet.
        parquet_dir: Output directory for Parquet files.
        documents: Optional document metadata for Parquet.
        text_units: Optional text unit records for Parquet.
        communities: Optional community records for Parquet.

    Returns:
        Combined stats dict with keys "neo4j", "parquet", and "errors".
    """
    output: dict[str, Any] = {"neo4j": {}, "parquet": {}, "errors": []}

    # ─── Neo4j write ───────────────────────────────────────────────────
    if neo4j_enabled:
        try:
            neo4j_stats = await publish_to_graph(
                results,
                database_uri=database_uri,
                database_name=database_name,
                username=username,
                password=password,
            )
            output["neo4j"] = neo4j_stats
        except Exception as e:
            logger.warning("Neo4j publish failed: %s", e)
            output["errors"].append({"backend": "neo4j", "error": str(e)})

    # ─── Parquet write ─────────────────────────────────────────────────
    if parquet_enabled:
        try:
            pq_stats = await publish_to_parquet(
                results,
                output_dir=parquet_dir,
                documents=documents,
                text_units=text_units,
                communities=communities,
            )
            output["parquet"] = pq_stats
        except Exception as e:
            logger.warning("Parquet publish failed: %s", e)
            output["errors"].append({"backend": "parquet", "error": str(e)})

    return output
