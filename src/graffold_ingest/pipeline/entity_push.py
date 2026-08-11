"""Entity push — accept pre-extracted entities and publish to graph.

Bypasses the LLM extraction stage. Runs: validate → resolve → publish → embed.
Designed for external systems (Agteria) that do their own extraction.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from ..connectors.base import ExtractionResult
from .embed import embed_and_upload
from .publish import publish_to_graph
from .resolve import resolve_entities

logger = logging.getLogger(__name__)

_ASYNC_THRESHOLD = 50


@dataclass
class EntityPushStats:
    """Statistics from an entity push operation."""

    nodes_created: int = 0
    nodes_merged: int = 0
    edges_created: int = 0
    embeddings_queued: int = 0


def _build_extraction_result(
    entities: list[dict[str, Any]],
    relationships: list[dict[str, Any]],
    source_run_id: str,
    source_system: str,
) -> ExtractionResult:
    """Convert entity/relationship input dicts into an ExtractionResult.

    Injects provenance into each node and edge.
    """
    nodes: list[dict[str, Any]] = []
    for entity in entities:
        node: dict[str, Any] = {**entity}
        # Ensure every node has an id
        if "id" not in node:
            node["id"] = str(uuid.uuid4())
        # Inject provenance into properties
        node["_extraction_method"] = source_system
        node["_source_doc_id"] = source_run_id
        if "source_problem_id" in node.get("properties", {}):
            node["_source_problem_id"] = node["properties"]["source_problem_id"]
        elif "_source_problem_id" in node:
            pass  # already set
        nodes.append(node)

    edges: list[dict[str, Any]] = []
    for rel in relationships:
        edge: dict[str, Any] = {**rel}
        # resolve.py expects "source"/"target" keys
        # publish.py expects "source_id"/"target_id" keys
        if "source" in edge:
            edge["source_id"] = edge["source"]
        if "target" in edge:
            edge["target_id"] = edge["target"]
        edges.append(edge)

    return ExtractionResult(
        nodes=nodes,
        edges=edges,
        source_doc_id=source_run_id,
    )


async def check_idempotency(
    source_run_id: str,
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "neo4j",
    password: str = "",
) -> bool:
    """Return True if source_run_id has already been processed."""
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(database_uri, auth=(username, password))
    try:
        async with driver.session(database=database_name) as session:
            result = await session.run(
                "MATCH (d:ProcessedDocument {doc_id: $id}) RETURN d LIMIT 1",
                {"id": source_run_id},
            )
            record = await result.single()
            return record is not None
    finally:
        await driver.close()


async def mark_processed(
    source_run_id: str,
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "neo4j",
    password: str = "",
) -> None:
    """Mark a source_run_id as processed in ProcessedDocument."""
    import time

    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(database_uri, auth=(username, password))
    try:
        async with driver.session(database=database_name) as session:
            await session.run(
                "MERGE (d:ProcessedDocument {doc_id: $id}) "
                "SET d.processed_at = $ts",
                {"id": source_run_id, "ts": time.time()},
            )
    finally:
        await driver.close()


async def process_entity_push(
    entities: list[dict[str, Any]],
    relationships: list[dict[str, Any]],
    source_run_id: str,
    source_system: str,
    project_id: str = "default",
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "neo4j",
    password: str = "",
) -> EntityPushStats:
    """Process a batch of pre-extracted entities end-to-end.

    Steps:
    1. Check idempotency — skip if source_run_id already processed
    2. Convert inputs to ExtractionResult format
    3. Resolve/deduplicate entities
    4. Publish to Neo4j graph with provenance
    5. Queue embeddings (best-effort)
    6. Mark as processed

    Returns stats on what was written.
    """
    # 1. Idempotency check
    already_done = await check_idempotency(
        source_run_id,
        database_uri=database_uri,
        database_name=database_name,
        username=username,
        password=password,
    )
    if already_done:
        logger.info("source_run_id=%s already processed, skipping", source_run_id)
        return EntityPushStats()

    # 2. Build ExtractionResult
    extraction = _build_extraction_result(
        entities=entities,
        relationships=relationships,
        source_run_id=source_run_id,
        source_system=source_system,
    )

    # 3. Resolve (dedup across this batch)
    resolved = resolve_entities([extraction])

    # 4. Publish to graph
    publish_stats = await publish_to_graph(
        results=resolved,
        database_uri=database_uri,
        database_name=database_name,
        username=username,
        password=password,
    )

    nodes_created = publish_stats.get("nodes_created", 0)
    edges_created = publish_stats.get("edges_created", 0)

    # 5. Queue embeddings (best-effort)
    embeddings_queued = 0
    all_nodes = [n for r in resolved for n in r.nodes]
    try:
        embeddings_queued = await embed_and_upload(
            nodes=all_nodes,
            database_uri=database_uri,
            database_name=database_name,
        )
    except Exception:
        logger.warning(
            "Embedding failed for source_run_id=%s, continuing",
            source_run_id,
            exc_info=True,
        )

    # 6. Mark processed
    await mark_processed(
        source_run_id,
        database_uri=database_uri,
        database_name=database_name,
        username=username,
        password=password,
    )

    return EntityPushStats(
        nodes_created=nodes_created,
        nodes_merged=0,  # TODO: track merges in resolve step
        edges_created=edges_created,
        embeddings_queued=embeddings_queued,
    )
