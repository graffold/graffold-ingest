"""FalkorDB backend — batched OpenCypher over the Redis protocol.

FalkorDB stores many named graphs per instance (perfect for per-program
serving: alltech / elanco / zoetis / master each a named graph). Writes use
batched UNWIND MERGE; reads use MATCH.

Requires: falkordb (pip install falkordb)

Config:
    FALKORDB_HOST (default localhost)
    FALKORDB_PORT (default 6379)
    graph_name — which named graph to target (required for per-program)
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Any

from ..connectors.base import ExtractionResult

logger = logging.getLogger(__name__)

BATCH_SIZE = 500  # FalkorDB handles large UNWIND batches well


class FalkorDBBackend:
    """Graph backend using FalkorDB (Redis-protocol, native multi-graph)."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        graph_name: str = "graffold",
        **kwargs: Any,
    ) -> None:
        self._host = host or os.getenv("FALKORDB_HOST", "localhost")
        self._port = port or int(os.getenv("FALKORDB_PORT", "6379"))
        self._graph_name = graph_name
        self._db = None
        self._graph = None

    @property
    def name(self) -> str:
        return "falkordb"

    @property
    def graph(self):
        """Lazy-connect and select the named graph."""
        if self._graph is None:
            from falkordb import FalkorDB

            self._db = FalkorDB(host=self._host, port=self._port)
            self._graph = self._db.select_graph(self._graph_name)
        return self._graph

    async def publish(
        self,
        results: list[ExtractionResult],
        **kwargs: Any,
    ) -> dict[str, int]:
        """Write entities + relationships to the named graph via batched UNWIND."""
        import asyncio

        nodes_written = 0
        edges_written = 0
        ingested_at = int(time.time() * 1000)

        def _write() -> tuple[int, int]:
            nw = ew = 0
            for result in results:
                version_hash = hashlib.sha256(
                    result.source_doc_id.encode()
                ).hexdigest()[:12]

                # Entities grouped by label, batched
                by_label: dict[str, list[dict]] = {}
                for node in result.nodes:
                    label = _safe_label(node.get("label", node.get("type", "Entity")))
                    by_label.setdefault(label, []).append({
                        "id": node["id"],
                        "name": node.get("name", node["id"]),
                        "type": node.get("type", label),
                        "description": (node.get("description") or "")[:500],
                        "doc_id": result.source_doc_id,
                        "ingested_at": ingested_at,
                        "version_hash": version_hash,
                    })
                for label, batch in by_label.items():
                    for i in range(0, len(batch), BATCH_SIZE):
                        chunk = batch[i:i + BATCH_SIZE]
                        q = (
                            "UNWIND $rows AS r "
                            f"MERGE (e:`{label}` {{id: r.id}}) "
                            "SET e.name = r.name, e.type = r.type, "
                            "e.description = r.description, e.doc_id = r.doc_id, "
                            "e.ingested_at = r.ingested_at, e.version_hash = r.version_hash"
                        )
                        self.graph.query(q, {"rows": chunk})
                        nw += len(chunk)

                # Relationships grouped by type, batched
                by_type: dict[str, list[dict]] = {}
                for edge in result.edges:
                    rel = _safe_label(edge.get("type", "RELATED_TO"))
                    by_type.setdefault(rel, []).append({
                        "src": edge.get("source_id", edge.get("source", "")),
                        "tgt": edge.get("target_id", edge.get("target", "")),
                        "description": (edge.get("description") or "")[:300],
                        "doc_id": result.source_doc_id,
                    })
                for rel, batch in by_type.items():
                    for i in range(0, len(batch), BATCH_SIZE):
                        chunk = batch[i:i + BATCH_SIZE]
                        q = (
                            "UNWIND $rows AS r "
                            "MATCH (a {id: r.src}) MATCH (b {id: r.tgt}) "
                            f"MERGE (a)-[x:`{rel}`]->(b) "
                            "SET x.description = r.description, x.doc_id = r.doc_id"
                        )
                        try:
                            self.graph.query(q, {"rows": chunk})
                            ew += len(chunk)
                        except Exception as e:
                            logger.warning("FalkorDB edge batch failed (%s): %s", rel, str(e)[:80])
            return nw, ew

        nodes_written, edges_written = await asyncio.to_thread(_write)
        logger.info("FalkorDB[%s]: wrote %d nodes, %d edges",
                    self._graph_name, nodes_written, edges_written)
        return {"nodes_created": nodes_written, "edges_created": edges_written}

    async def query_entities(
        self,
        search_term: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        import asyncio

        def _q():
            res = self.graph.query(
                "MATCH (e) WHERE toLower(e.name) CONTAINS toLower($t) "
                "RETURN e.id AS id, e.name AS name, e.type AS type LIMIT $lim",
                {"t": search_term, "lim": limit},
            )
            return [{"id": r[0], "name": r[1], "type": r[2]} for r in res.result_set]

        try:
            return await asyncio.to_thread(_q)
        except Exception as e:
            logger.warning("FalkorDB query failed: %s", e)
            return []

    async def get_neighbors(
        self,
        entity_id: str,
        *,
        max_hops: int = 1,
    ) -> dict[str, Any]:
        import asyncio

        def _q():
            res = self.graph.query(
                f"MATCH (e {{id: $id}})-[*1..{max_hops}]-(n) "
                "RETURN DISTINCT n.id AS id, n.name AS name, n.type AS type LIMIT 50",
                {"id": entity_id},
            )
            return {"neighbors": [{"id": r[0], "name": r[1], "type": r[2]} for r in res.result_set]}

        try:
            return await asyncio.to_thread(_q)
        except Exception as e:
            logger.warning("FalkorDB neighbor query failed: %s", e)
            return {"neighbors": []}

    async def health_check(self) -> bool:
        import asyncio

        try:
            await asyncio.to_thread(lambda: self.graph.query("RETURN 1"))
            return True
        except Exception:
            return False


def _safe_label(s: str) -> str:
    """Sanitize a label/rel-type for Cypher backtick injection safety."""
    import re

    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", (s or "Entity").strip())
    if not cleaned or not cleaned[0].isalpha():
        cleaned = "E_" + cleaned
    return cleaned[:60]
