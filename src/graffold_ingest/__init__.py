"""graffold-ingest — turn any corpus into a canonical knowledge graph.

Public SDK surface. Import high-level verbs and core types directly:

    import asyncio
    from graffold_ingest import (
        Document, chunk_documents, extract_entities,
        EntityResolver, harmonize_graph, publish_to_parquet,
        read_parquet_graph, query_graph, get_backend,
    )

    async def build():
        docs = [Document(id="1", content="TP53 inhibits MDM2.", source_type="text")]
        results = await extract_entities(docs, llm_service="bedrock-llama")
        await publish_to_parquet(results, output_dir="./graph")

    asyncio.run(build())

Everything here is a stable public API. Submodules (graffold_ingest.pipeline.*,
.connectors.*, .backends.*) remain importable for advanced use, but names
prefixed with "_" are private and may change without notice.

See docs/ETEC_RUNBOOK.md for an end-to-end example and the CLI (`graffold-ingest
--help`) for the command-line surface.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

__version__ = "0.3.0"

# ─── Public API ──────────────────────────────────────────────────────────────
# Lazy-loaded so `import graffold_ingest` stays light — heavy deps (torch,
# boto3, pyarrow) load only when the symbol is actually accessed.

_EXPORTS: dict[str, tuple[str, str]] = {
    # Core types
    "Document": ("graffold_ingest.connectors.base", "Document"),
    "ExtractionResult": ("graffold_ingest.connectors.base", "ExtractionResult"),
    # Ingestion pipeline
    "chunk_documents": ("graffold_ingest.pipeline.chunk", "chunk_documents"),
    "chunk_tabular": ("graffold_ingest.pipeline.tabular", "chunk_tabular"),
    "extract_entities": ("graffold_ingest.pipeline.extract", "extract_entities"),
    "extract_entities_parallel": ("graffold_ingest.pipeline.extract", "extract_entities_parallel"),
    # Resolution & harmonization
    "EntityResolver": ("graffold_ingest.resolvers.local", "EntityResolver"),
    "harmonize_graph": ("graffold_ingest.pipeline.harmonize", "harmonize_graph"),
    "HarmonizeReport": ("graffold_ingest.pipeline.harmonize", "HarmonizeReport"),
    # Communities
    "detect_communities": ("graffold_ingest.pipeline.community", "detect_communities"),
    "summarize_communities": ("graffold_ingest.pipeline.community", "summarize_communities"),
    # Storage
    "publish_to_parquet": ("graffold_ingest.pipeline.publish_parquet", "publish_to_parquet"),
    "read_parquet_graph": ("graffold_ingest.pipeline.publish_parquet", "read_parquet_graph"),
    # Backends
    "get_backend": ("graffold_ingest.backends", "get_backend"),
    "GraphBackend": ("graffold_ingest.backends", "GraphBackend"),
    # Query
    "query_graph": ("graffold_ingest.pipeline.query_agent", "query_graph"),
    "QueryResult": ("graffold_ingest.pipeline.query_agent", "QueryResult"),
    # Literature connectors
    "PubMedConnector": ("graffold_ingest.connectors.pubmed", "PubMedConnector"),
    "EuropePMCConnector": ("graffold_ingest.connectors.europepmc", "EuropePMCConnector"),
}

__all__ = ["__version__", *sorted(_EXPORTS)]


def __getattr__(name: str):
    """PEP 562 lazy attribute access for the public API."""
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'graffold_ingest' has no attribute {name!r}")
    module_path, attr = target
    import importlib

    obj = getattr(importlib.import_module(module_path), attr)
    globals()[name] = obj  # cache for next access
    return obj


def __dir__() -> list[str]:
    return __all__


if TYPE_CHECKING:
    # Static-analysis / IDE autocompletion — mirrors _EXPORTS.
    from graffold_ingest.backends import GraphBackend, get_backend
    from graffold_ingest.connectors.base import Document, ExtractionResult
    from graffold_ingest.connectors.europepmc import EuropePMCConnector
    from graffold_ingest.connectors.pubmed import PubMedConnector
    from graffold_ingest.pipeline.chunk import chunk_documents
    from graffold_ingest.pipeline.community import detect_communities, summarize_communities
    from graffold_ingest.pipeline.extract import extract_entities, extract_entities_parallel
    from graffold_ingest.pipeline.harmonize import HarmonizeReport, harmonize_graph
    from graffold_ingest.pipeline.publish_parquet import publish_to_parquet, read_parquet_graph
    from graffold_ingest.pipeline.query_agent import QueryResult, query_graph
    from graffold_ingest.pipeline.tabular import chunk_tabular
    from graffold_ingest.resolvers.local import EntityResolver
