"""Pipeline orchestrator — runs the full KG building pipeline with stage timing.

The single async entry point for ingestion:
    result = await run_pipeline(source="web", url="https://...", service="bedrock")

Stages: fetch → chunk → extract → resolve → publish
Each stage is timed and reported.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from ..connectors import CONNECTORS, Document
from .chunk import chunk_documents
from .extract import extract_entities
from .resolve import resolve_entities
from .schema import KGSchema

logger = logging.getLogger(__name__)


@dataclass
class StageResult:
    """Timing and stats for a single pipeline stage."""

    name: str
    elapsed_seconds: float = 0.0
    items_in: int = 0
    items_out: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class PipelineResult:
    """Full result of a pipeline run."""

    status: str  # "completed", "failed", "cancelled"
    source_type: str
    elapsed_seconds: float = 0.0
    documents_fetched: int = 0
    chunks_created: int = 0
    nodes_extracted: int = 0
    edges_extracted: int = 0
    nodes_published: int = 0
    edges_published: int = 0
    stages: list[StageResult] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)

    def to_report(self) -> dict[str, Any]:
        """Serialize to a run report dict."""
        return {
            "status": self.status,
            "source_type": self.source_type,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "documents_fetched": self.documents_fetched,
            "chunks_created": self.chunks_created,
            "nodes_extracted": self.nodes_extracted,
            "edges_extracted": self.edges_extracted,
            "nodes_published": self.nodes_published,
            "edges_published": self.edges_published,
            "stages": [
                {
                    "name": s.name,
                    "elapsed": round(s.elapsed_seconds, 2),
                    "items_in": s.items_in,
                    "items_out": s.items_out,
                }
                for s in self.stages
            ],
            "errors": self.errors,
        }


async def run_pipeline(
    source: str,
    *,
    url: str = "",
    path: str = "",
    schema_path: str | None = None,
    llm_service: str = "bedrock",
    llm_model: str = "",
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "neo4j",
    password: str = "",
    chunk_size: int = 2000,
    progress_callback: Callable[..., Any] | None = None,
    cancellation_event: asyncio.Event | None = None,
) -> PipelineResult:
    """Run the full ingestion pipeline.

    Args:
        source: Connector name ("web", "pdf", "api", "csv", "database")
        url: URL for web/api connectors
        path: File path for pdf/csv/database connectors
        schema_path: Path to schema YAML (None = use default)
        llm_service: LLM backend ("bedrock", "openai", "ollama")
        llm_model: Model ID override
        database_uri: Graph database URI
        database_name: Database name
        username/password: Auth for graph DB
        chunk_size: Max chars per chunk
        progress_callback: Called with (stage, items_done, items_total)
        cancellation_event: Set to cancel mid-pipeline

    Returns:
        PipelineResult with full timing and stats
    """
    t0 = time.time()
    errors: list[dict[str, Any]] = []
    stages: list[StageResult] = []

    def _cancelled() -> bool:
        return cancellation_event is not None and cancellation_event.is_set()

    def _progress(stage: str, done: int, total: int) -> None:
        if progress_callback:
            progress_callback(stage=stage, items_processed=done, total_items=total)

    # Load schema
    schema = KGSchema.load(schema_path) if schema_path else KGSchema.load()

    # ─── Stage 1: Fetch ────────────────────────────────────────────────
    stage_t = time.time()
    _progress("fetching", 0, 1)

    connector_cls = CONNECTORS.get(source)
    if not connector_cls:
        return PipelineResult(
            status="failed", source_type=source,
            errors=[{"stage": "fetching", "error": f"Unknown source: {source}"}],
            elapsed_seconds=time.time() - t0,
        )

    connector = connector_cls()
    kwargs: dict[str, Any] = {}
    if url:
        kwargs["url"] = url
    if path:
        kwargs["path"] = path

    try:
        docs = await connector.fetch(**kwargs)
    except Exception as e:
        return PipelineResult(
            status="failed", source_type=source,
            errors=[{"stage": "fetching", "error": str(e)}],
            elapsed_seconds=time.time() - t0,
        )

    stages.append(StageResult("fetching", time.time() - stage_t, 1, len(docs)))
    _progress("fetching", len(docs), len(docs))

    if not docs or _cancelled():
        return PipelineResult(
            status="cancelled" if _cancelled() else "completed",
            source_type=source, documents_fetched=len(docs),
            stages=stages, elapsed_seconds=time.time() - t0,
        )

    # ─── Stage 2: Chunk ────────────────────────────────────────────────
    stage_t = time.time()
    _progress("chunking", 0, len(docs))
    chunks = chunk_documents(docs, chunk_size=chunk_size)
    stages.append(StageResult("chunking", time.time() - stage_t, len(docs), len(chunks)))
    _progress("chunking", len(docs), len(docs))

    if _cancelled():
        return PipelineResult(status="cancelled", source_type=source, elapsed_seconds=time.time() - t0)

    # ─── Stage 3: Extract ──────────────────────────────────────────────
    stage_t = time.time()
    _progress("extracting", 0, len(chunks))

    try:
        results = await extract_entities(chunks, llm_service=llm_service, model_id=llm_model)
    except Exception as e:
        errors.append({"stage": "extracting", "error": str(e)})
        results = []

    # Validate against schema
    for r in results:
        r.nodes = schema.validate_entities(r.nodes)
        r.edges = schema.validate_relationships(r.edges, r.nodes)

    total_nodes = sum(len(r.nodes) for r in results)
    total_edges = sum(len(r.edges) for r in results)
    stages.append(StageResult("extracting", time.time() - stage_t, len(chunks), total_nodes + total_edges))
    _progress("extracting", len(chunks), len(chunks))

    if _cancelled():
        return PipelineResult(status="cancelled", source_type=source, elapsed_seconds=time.time() - t0)

    # ─── Stage 4: Resolve ──────────────────────────────────────────────
    stage_t = time.time()
    results = resolve_entities(results)
    resolved_nodes = sum(len(r.nodes) for r in results)
    stages.append(StageResult("resolving", time.time() - stage_t, total_nodes, resolved_nodes))

    if _cancelled():
        return PipelineResult(status="cancelled", source_type=source, elapsed_seconds=time.time() - t0)

    # ─── Stage 5: Publish ──────────────────────────────────────────────
    stage_t = time.time()
    _progress("publishing", 0, len(results))

    try:
        from .publish import publish_to_graph

        counts = await publish_to_graph(
            results,
            database_uri=database_uri,
            database_name=database_name,
            username=username,
            password=password,
        )
        nodes_pub = counts.get("nodes_created", 0)
        edges_pub = counts.get("edges_created", 0)
    except Exception as e:
        errors.append({"stage": "publishing", "error": str(e)})
        nodes_pub = edges_pub = 0

    stages.append(StageResult("publishing", time.time() - stage_t, resolved_nodes, nodes_pub + edges_pub))
    _progress("publishing", len(results), len(results))

    elapsed = time.time() - t0
    logger.info(
        "Pipeline complete: %d docs → %d nodes, %d edges in %.1fs",
        len(docs), nodes_pub, edges_pub, elapsed,
    )

    return PipelineResult(
        status="completed",
        source_type=source,
        elapsed_seconds=elapsed,
        documents_fetched=len(docs),
        chunks_created=len(chunks),
        nodes_extracted=total_nodes,
        edges_extracted=total_edges,
        nodes_published=nodes_pub,
        edges_published=edges_pub,
        stages=stages,
        errors=errors,
    )
