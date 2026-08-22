"""AWS Neptune backend — batched OpenCypher via boto3 neptunedata.

Uses UNWIND for batched writes (50 entities/rels per request).
Auth via IAM (standard boto3 credential chain).

Ported from bioingest.pipeline.writers.NeptuneWriter.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any

from ..connectors.base import ExtractionResult

logger = logging.getLogger(__name__)

BATCH_SIZE = 50


class NeptuneBackend:
    """Graph backend using AWS Neptune via boto3 neptunedata (OpenCypher)."""

    def __init__(
        self,
        endpoint: str | None = None,
        port: int | None = None,
        region: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._endpoint = endpoint or os.getenv("NEPTUNE_ENDPOINT", "")
        self._port = port or int(os.getenv("NEPTUNE_PORT", "8182"))
        self._region = region or os.getenv("AWS_REGION", "us-east-1")
        self._client = None

    @property
    def name(self) -> str:
        return "neptune"

    @property
    def client(self):
        """Lazy-init boto3 neptunedata client."""
        if self._client is None:
            import boto3
            from botocore.config import Config

            cfg = Config(
                read_timeout=300,
                connect_timeout=30,
                retries={"max_attempts": 2},
            )
            self._client = boto3.client(
                "neptunedata",
                region_name=self._region,
                endpoint_url=f"https://{self._endpoint}:{self._port}",
                config=cfg,
            )
        return self._client

    async def publish(
        self,
        results: list[ExtractionResult],
        **kwargs: Any,
    ) -> dict[str, int]:
        """Write entities and relationships via batched UNWIND queries."""
        nodes_written = 0
        rels_written = 0
        ingested_at = int(time.time() * 1000)

        for result in results:
            version_hash = hashlib.sha256(
                result.source_doc_id.encode()
            ).hexdigest()[:12]

            # ─── Entities (grouped by label, batched) ──────────────────────
            by_label: dict[str, list[dict]] = {}
            for node in result.nodes:
                label = node.get("label", node.get("type", "Entity"))
                by_label.setdefault(label, []).append({
                    "id": node["id"],
                    "name": node.get("name", node["id"]),
                    "doc_id": result.source_doc_id,
                    "ingested_at": ingested_at,
                    "version_hash": version_hash,
                })

            for label, nodes in by_label.items():
                for i in range(0, len(nodes), BATCH_SIZE):
                    batch = nodes[i:i + BATCH_SIZE]
                    query = (
                        "UNWIND $nodes AS n "
                        f"MERGE (e:`{label}` {{id: n.id}}) "
                        "SET e.name = n.name, e.doc_id = n.doc_id, "
                        "e.ingested_at = n.ingested_at, e.version_hash = n.version_hash"
                    )
                    try:
                        self.client.execute_open_cypher_query(
                            openCypherQuery=query,
                            parameters=json.dumps({"nodes": batch}),
                        )
                        nodes_written += len(batch)
                    except Exception as e:
                        logger.warning("Neptune entity batch failed: %s", e)

            # ─── Relationships (grouped by type, batched) ──────────────────
            by_type: dict[str, list[dict]] = {}
            for edge in result.edges:
                rel_type = edge.get("type", "RELATED_TO")
                by_type.setdefault(rel_type, []).append({
                    "source_id": edge.get("source_id", edge.get("source", "")),
                    "target_id": edge.get("target_id", edge.get("target", "")),
                    "doc_id": result.source_doc_id,
                })

            for rel_type, rels in by_type.items():
                for i in range(0, len(rels), BATCH_SIZE):
                    batch = rels[i:i + BATCH_SIZE]
                    query = (
                        "UNWIND $rels AS r "
                        "MATCH (a {id: r.source_id}) "
                        "MATCH (b {id: r.target_id}) "
                        f"MERGE (a)-[:`{rel_type}`]->(b) "
                        "SET r.doc_id = r.doc_id"
                    )
                    try:
                        self.client.execute_open_cypher_query(
                            openCypherQuery=query,
                            parameters=json.dumps({"rels": batch}),
                        )
                        rels_written += len(batch)
                    except Exception as e:
                        logger.warning("Neptune rel batch failed: %s", e)

        logger.info("Neptune: wrote %d nodes, %d rels", nodes_written, rels_written)
        return {"nodes_created": nodes_written, "edges_created": rels_written}

    async def query_entities(
        self,
        search_term: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        query = (
            "MATCH (e) WHERE toLower(e.name) CONTAINS toLower($term) "
            "RETURN e.id AS id, e.name AS name, labels(e) AS labels "
            "LIMIT $limit"
        )
        try:
            resp = self.client.execute_open_cypher_query(
                openCypherQuery=query,
                parameters=json.dumps({"term": search_term, "limit": limit}),
            )
            return resp.get("results", [])
        except Exception as e:
            logger.warning("Neptune query failed: %s", e)
            return []

    async def get_neighbors(
        self,
        entity_id: str,
        *,
        max_hops: int = 1,
    ) -> dict[str, Any]:
        query = (
            "MATCH (e {id: $id})-[r*1..$hops]-(n) "
            "RETURN e.name AS source, collect(DISTINCT n.name) AS neighbors"
        )
        try:
            resp = self.client.execute_open_cypher_query(
                openCypherQuery=query,
                parameters=json.dumps({"id": entity_id, "hops": max_hops}),
            )
            return resp.get("results", [{}])[0] if resp.get("results") else {"neighbors": []}
        except Exception as e:
            logger.warning("Neptune neighbor query failed: %s", e)
            return {"neighbors": []}

    async def health_check(self) -> bool:
        try:
            self.client.execute_open_cypher_query(
                openCypherQuery="RETURN 1",
                parameters="{}",
            )
            return True
        except Exception:
            return False
