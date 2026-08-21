"""Neo4j backend — wraps existing publish_to_graph for the GraphBackend protocol."""

from __future__ import annotations

import logging
import os
from typing import Any

from ..connectors.base import ExtractionResult

logger = logging.getLogger(__name__)


class Neo4jBackend:
    """Graph backend using Neo4j/Memgraph via Bolt protocol."""

    def __init__(
        self,
        database_uri: str | None = None,
        database_name: str | None = None,
        username: str | None = None,
        password: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._uri = database_uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self._database = database_name or os.getenv("NEO4J_DATABASE", "neo4j")
        self._username = username or os.getenv("NEO4J_USER", "neo4j")
        self._password = password or os.getenv("NEO4J_PASSWORD", "")

    @property
    def name(self) -> str:
        return "neo4j"

    async def publish(
        self,
        results: list[ExtractionResult],
        **kwargs: Any,
    ) -> dict[str, int]:
        from ..pipeline.publish import publish_to_graph

        return await publish_to_graph(
            results,
            database_uri=self._uri,
            database_name=self._database,
            username=self._username,
            password=self._password,
        )

    async def query_entities(
        self,
        search_term: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        from neo4j import AsyncGraphDatabase

        driver = AsyncGraphDatabase.driver(self._uri, auth=(self._username, self._password))
        try:
            async with driver.session(database=self._database) as session:
                result = await session.run(
                    "MATCH (e) WHERE toLower(e.name) CONTAINS toLower($term) "
                    "RETURN e.id AS id, e.name AS name, labels(e) AS labels "
                    "LIMIT $limit",
                    {"term": search_term, "limit": limit},
                )
                return await result.data()
        finally:
            await driver.close()

    async def get_neighbors(
        self,
        entity_id: str,
        *,
        max_hops: int = 1,
    ) -> dict[str, Any]:
        from neo4j import AsyncGraphDatabase

        driver = AsyncGraphDatabase.driver(self._uri, auth=(self._username, self._password))
        try:
            async with driver.session(database=self._database) as session:
                result = await session.run(
                    "MATCH (e {id: $id})-[r*1..$hops]-(n) "
                    "RETURN e, collect(DISTINCT n) AS neighbors, "
                    "collect(DISTINCT r) AS paths",
                    {"id": entity_id, "hops": max_hops},
                )
                record = await result.single()
                if record:
                    return dict(record)
                return {"neighbors": [], "paths": []}
        finally:
            await driver.close()

    async def health_check(self) -> bool:
        try:
            from neo4j import AsyncGraphDatabase

            driver = AsyncGraphDatabase.driver(
                self._uri, auth=(self._username, self._password)
            )
            async with driver.session(database=self._database) as session:
                await session.run("RETURN 1")
            await driver.close()
            return True
        except Exception:
            return False
