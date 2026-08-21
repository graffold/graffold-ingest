"""DuckDB backend — query Parquet files via SQL. Read-heavy, no write path.

DuckDB queries Parquet directly without loading into memory.
Ideal for local dev, analytics, and environments without a graph DB.

Configuration:
    PARQUET_DIR: Directory with Parquet files (default: ~/.graffold/parquet)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from ..connectors.base import ExtractionResult

logger = logging.getLogger(__name__)


class DuckDBBackend:
    """Graph backend using DuckDB over Parquet files.

    Primarily a query backend — publishes via Parquet writer,
    queries via DuckDB SQL over those same files.
    """

    def __init__(
        self,
        parquet_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._parquet_dir = Path(
            parquet_dir or os.getenv("PARQUET_DIR", str(Path.home() / ".graffold" / "parquet"))
        )

    @property
    def name(self) -> str:
        return "duckdb"

    async def publish(
        self,
        results: list[ExtractionResult],
        **kwargs: Any,
    ) -> dict[str, int]:
        """Publish via Parquet writer (DuckDB reads those files)."""
        from ..pipeline.publish_parquet import publish_to_parquet

        return await publish_to_parquet(results, output_dir=self._parquet_dir)

    async def query_entities(
        self,
        search_term: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        import duckdb

        entities_path = self._parquet_dir / "entities.parquet"
        if not entities_path.exists():
            return []

        con = duckdb.connect()
        try:
            result = con.execute(
                f"SELECT id, name, type FROM read_parquet('{entities_path}') "
                "WHERE lower(name) LIKE '%' || lower(?) || '%' "
                "LIMIT ?",
                [search_term, limit],
            ).fetchall()
            return [
                {"id": r[0], "name": r[1], "type": r[2]} for r in result
            ]
        finally:
            con.close()

    async def get_neighbors(
        self,
        entity_id: str,
        *,
        max_hops: int = 1,
    ) -> dict[str, Any]:
        """Get neighbors using recursive CTE over relationships Parquet."""
        import duckdb

        rels_path = self._parquet_dir / "relationships.parquet"
        entities_path = self._parquet_dir / "entities.parquet"
        if not rels_path.exists() or not entities_path.exists():
            return {"neighbors": []}

        con = duckdb.connect()
        try:
            # Simple 1-hop for now (recursive CTE for multi-hop adds complexity)
            if max_hops == 1:
                result = con.execute(
                    f"""
                    SELECT DISTINCT e.id, e.name, e.type, r.type AS rel_type
                    FROM read_parquet('{rels_path}') r
                    JOIN read_parquet('{entities_path}') e
                      ON (e.id = r.target_id AND r.source_id = ?)
                      OR (e.id = r.source_id AND r.target_id = ?)
                    LIMIT 50
                    """,
                    [entity_id, entity_id],
                ).fetchall()
            else:
                # Multi-hop via recursive CTE
                result = con.execute(
                    f"""
                    WITH RECURSIVE hops AS (
                        SELECT target_id AS id, type AS rel_type, 1 AS depth
                        FROM read_parquet('{rels_path}')
                        WHERE source_id = ?
                        UNION
                        SELECT source_id AS id, type AS rel_type, 1 AS depth
                        FROM read_parquet('{rels_path}')
                        WHERE target_id = ?
                        UNION ALL
                        SELECT
                            CASE WHEN r.source_id = h.id THEN r.target_id ELSE r.source_id END,
                            r.type,
                            h.depth + 1
                        FROM hops h
                        JOIN read_parquet('{rels_path}') r
                          ON r.source_id = h.id OR r.target_id = h.id
                        WHERE h.depth < ?
                    )
                    SELECT DISTINCT e.id, e.name, e.type
                    FROM hops h
                    JOIN read_parquet('{entities_path}') e ON e.id = h.id
                    WHERE e.id != ?
                    LIMIT 100
                    """,
                    [entity_id, entity_id, max_hops, entity_id],
                ).fetchall()

            return {
                "neighbors": [
                    {"id": r[0], "name": r[1], "type": r[2]} for r in result
                ]
            }
        finally:
            con.close()

    async def health_check(self) -> bool:
        return self._parquet_dir.exists()
