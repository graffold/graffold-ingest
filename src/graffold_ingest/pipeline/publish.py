"""Publish extracted entities to a graph database."""

from __future__ import annotations

from typing import Any

from ..connectors.base import ExtractionResult


async def publish_to_graph(
    results: list[ExtractionResult],
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "neo4j",
    password: str = "",
) -> dict[str, int]:
    """Write extracted nodes and edges to a Cypher-compatible graph database.

    Returns counts of created nodes and edges.
    """
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(database_uri, auth=(username, password))
    nodes_created = 0
    edges_created = 0

    async with driver.session(database=database_name) as session:
        for result in results:
            for node in result.nodes:
                cypher = (
                    f"MERGE (n:{node.get('label', 'Entity')} {{id: $id}}) "
                    f"SET n += $props"
                )
                props = {k: v for k, v in node.items() if k not in ("id", "label")}
                props["name"] = node.get("name", node.get("id", ""))
                await session.run(cypher, {"id": node["id"], "props": props})
                nodes_created += 1

            for edge in result.edges:
                cypher = (
                    "MATCH (a {id: $source}), (b {id: $target}) "
                    f"MERGE (a)-[r:{edge.get('type', 'RELATED_TO')}]->(b) "
                    "SET r += $props"
                )
                props = {k: v for k, v in edge.get("properties", {}).items()}
                await session.run(
                    cypher,
                    {"source": edge["source"], "target": edge["target"], "props": props},
                )
                edges_created += 1

    await driver.close()
    return {"nodes_created": nodes_created, "edges_created": edges_created}
