"""Google Cloud Spanner Graph backend — property graph on Cloud Spanner.

Spanner Graph maps relational tables to graph nodes/edges via a PROPERTY GRAPH schema.
Writes go to tables (batch mutations); reads use GQL via execute_sql.

Requires: google-cloud-spanner

Configuration:
    SPANNER_INSTANCE: Spanner instance ID
    SPANNER_DATABASE: Database ID
    GCP_PROJECT: Google Cloud project ID
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Any

from ..connectors.base import ExtractionResult

logger = logging.getLogger(__name__)

BATCH_SIZE = 500  # Spanner handles large batches well


class SpannerGraphBackend:
    """Graph backend using Google Cloud Spanner Graph (GQL + batch mutations)."""

    def __init__(
        self,
        instance_id: str | None = None,
        database_id: str | None = None,
        project: str | None = None,
        graph_name: str = "KnowledgeGraph",
        **kwargs: Any,
    ) -> None:
        self._instance_id = instance_id or os.getenv("SPANNER_INSTANCE", "")
        self._database_id = database_id or os.getenv("SPANNER_DATABASE", "")
        self._project = project or os.getenv("GCP_PROJECT", "")
        self._graph_name = graph_name
        self._db = None

    @property
    def name(self) -> str:
        return "spanner"

    @property
    def db(self):
        """Lazy-init Spanner database handle."""
        if self._db is None:
            from google.cloud import spanner

            client = spanner.Client(project=self._project)
            instance = client.instance(self._instance_id)
            self._db = instance.database(self._database_id)
        return self._db

    async def publish(
        self,
        results: list[ExtractionResult],
        **kwargs: Any,
    ) -> dict[str, int]:
        """Write entities and relationships to Spanner tables via batch mutations."""
        nodes_written = 0
        edges_written = 0
        ingested_at = int(time.time() * 1000)

        for result in results:
            version_hash = hashlib.sha256(
                result.source_doc_id.encode()
            ).hexdigest()[:12]

            # ─── Entities ──────────────────────────────────────────────────
            entity_rows = []
            for node in result.nodes:
                entity_rows.append((
                    node["id"],
                    node.get("name", node["id"]),
                    node.get("label", node.get("type", "Entity")),
                    result.source_doc_id,
                    str(ingested_at),
                    version_hash,
                ))

            if entity_rows:
                with self.db.batch() as batch:
                    for i in range(0, len(entity_rows), BATCH_SIZE):
                        batch.insert_or_update(
                            table="Entity",
                            columns=("id", "name", "type", "doc_id", "ingested_at", "version_hash"),
                            values=entity_rows[i:i + BATCH_SIZE],
                        )
                nodes_written += len(entity_rows)

            # ─── Relationships ─────────────────────────────────────────────
            edge_rows = []
            for edge in result.edges:
                source_id = edge.get("source_id", edge.get("source", ""))
                target_id = edge.get("target_id", edge.get("target", ""))
                rel_type = edge.get("type", "RELATED_TO")
                edge_rows.append((
                    source_id,
                    target_id,
                    rel_type,
                    result.source_doc_id,
                    str(ingested_at),
                ))

            if edge_rows:
                with self.db.batch() as batch:
                    for i in range(0, len(edge_rows), BATCH_SIZE):
                        batch.insert_or_update(
                            table="Relationship",
                            columns=("source_id", "target_id", "type", "doc_id", "ingested_at"),
                            values=edge_rows[i:i + BATCH_SIZE],
                        )
                edges_written += len(edge_rows)

        logger.info("Spanner: wrote %d nodes, %d edges", nodes_written, edges_written)
        return {"nodes_created": nodes_written, "edges_created": edges_written}

    async def query_entities(
        self,
        search_term: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Search entities using GQL pattern matching."""
        gql = (
            f"GRAPH {self._graph_name} "
            "MATCH (e:Entity) "
            "WHERE LOWER(e.name) LIKE CONCAT('%', LOWER(@term), '%') "
            "RETURN e.id AS id, e.name AS name, e.type AS type "
            "LIMIT @limit"
        )
        from google.cloud.spanner_v1 import param_types

        with self.db.snapshot() as snapshot:
            results = snapshot.execute_sql(
                gql,
                params={"term": search_term, "limit": limit},
                param_types={"term": param_types.STRING, "limit": param_types.INT64},
            )
            return [{"id": row[0], "name": row[1], "type": row[2]} for row in results]

    async def get_neighbors(
        self,
        entity_id: str,
        *,
        max_hops: int = 1,
    ) -> dict[str, Any]:
        """Get neighbors via GQL path traversal."""
        # GQL quantified path pattern for variable-length hops
        gql = (
            f"GRAPH {self._graph_name} "
            "MATCH (src:Entity {id: @id})-[r:Relates]->{1,@hops}(neighbor:Entity) "
            "RETURN DISTINCT neighbor.id AS id, neighbor.name AS name, neighbor.type AS type "
            "LIMIT 50"
        )
        from google.cloud.spanner_v1 import param_types

        try:
            with self.db.snapshot() as snapshot:
                results = snapshot.execute_sql(
                    gql,
                    params={"id": entity_id, "hops": max_hops},
                    param_types={"id": param_types.STRING, "hops": param_types.INT64},
                )
                neighbors = [{"id": row[0], "name": row[1], "type": row[2]} for row in results]
                return {"neighbors": neighbors}
        except Exception as e:
            logger.warning("Spanner neighbor query failed: %s", e)
            return {"neighbors": []}

    async def health_check(self) -> bool:
        try:
            with self.db.snapshot() as snapshot:
                list(snapshot.execute_sql("SELECT 1"))
            return True
        except Exception:
            return False

    @staticmethod
    def schema_ddl(graph_name: str = "KnowledgeGraph") -> list[str]:
        """Return DDL statements to create the required tables + graph schema.

        Run these once via Spanner admin API or console.
        """
        return [
            """CREATE TABLE IF NOT EXISTS Entity (
                id          STRING(MAX) NOT NULL,
                name        STRING(MAX),
                type        STRING(MAX),
                doc_id      STRING(MAX),
                ingested_at STRING(MAX),
                version_hash STRING(MAX),
            ) PRIMARY KEY (id)""",
            """CREATE TABLE IF NOT EXISTS Relationship (
                source_id   STRING(MAX) NOT NULL,
                target_id   STRING(MAX) NOT NULL,
                type        STRING(MAX),
                doc_id      STRING(MAX),
                ingested_at STRING(MAX),
                FOREIGN KEY (source_id) REFERENCES Entity(id),
                FOREIGN KEY (target_id) REFERENCES Entity(id),
            ) PRIMARY KEY (source_id, target_id, type)""",
            f"""CREATE OR REPLACE PROPERTY GRAPH {graph_name}
            NODE TABLES (
                Entity
                    KEY (id)
                    LABEL Entity
                    PROPERTIES (name, type, doc_id, ingested_at, version_hash)
            )
            EDGE TABLES (
                Relationship
                    KEY (source_id, target_id, type)
                    SOURCE KEY (source_id) REFERENCES Entity(id)
                    DESTINATION KEY (target_id) REFERENCES Entity(id)
                    LABEL Relates
                    PROPERTIES (type, doc_id, ingested_at)
            )""",
        ]
