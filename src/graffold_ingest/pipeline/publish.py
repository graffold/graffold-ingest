"""Publish extracted entities to a graph database with provenance tracking.

Every node and edge written carries provenance metadata:
- source_doc_id: which document it came from
- ingested_at: timestamp
- extraction_method: "llm"
- version_hash: content fingerprint
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

from ..connectors.base import ExtractionResult


async def publish_to_graph(
    results: list[ExtractionResult],
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "neo4j",
    password: str = "",
) -> dict[str, int]:
    """Write extracted nodes and edges to a Cypher-compatible graph DB.

    Adds provenance metadata to every node and edge:
    - _source_doc_id, _ingested_at, _extraction_method, _version_hash

    Returns counts of created nodes and edges.
    """
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(database_uri, auth=(username, password))
    nodes_created = 0
    edges_created = 0
    ingested_at = int(time.time() * 1000)

    async with driver.session(database=database_name) as session:
        for result in results:
            version_hash = hashlib.sha256(result.source_doc_id.encode()).hexdigest()[:12]

            for node in result.nodes:
                label = node.get("label", node.get("type", "Entity"))
                props: dict[str, Any] = {
                    k: v for k, v in node.items()
                    if k not in ("id", "label", "type")
                }
                props["name"] = node.get("name", node.get("id", ""))
                # Provenance
                props["_source_doc_id"] = result.source_doc_id
                props["_ingested_at"] = ingested_at
                props["_extraction_method"] = "llm"
                props["_version_hash"] = version_hash

                cypher = (
                    f"MERGE (n:{label} {{id: $id}}) "
                    f"SET n += $props"
                )
                await session.run(cypher, {"id": node["id"], "props": props})
                nodes_created += 1

            for edge in result.edges:
                rel_type = edge.get("type", "RELATED_TO")
                props: dict[str, Any] = {
                    k: v for k, v in edge.get("properties", {}).items()
                }
                # Provenance on edges
                props["_source_doc_id"] = result.source_doc_id
                props["_ingested_at"] = ingested_at
                props["_extraction_method"] = "llm"
                if edge.get("source_sentence"):
                    props["source_sentence"] = edge["source_sentence"]

                cypher = (
                    "MATCH (a {id: $source}), (b {id: $target}) "
                    f"MERGE (a)-[r:{rel_type}]->(b) "
                    "SET r += $props"
                )
                await session.run(
                    cypher,
                    {"source": edge["source_id"], "target": edge["target_id"], "props": props},
                )
                edges_created += 1

    await driver.close()
    return {"nodes_created": nodes_created, "edges_created": edges_created}
