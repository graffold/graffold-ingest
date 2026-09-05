"""Graph database backend protocol + loader registry.

Parquet is the source of truth. Graph backends are read/write adapters
that load from Parquet or accept ExtractionResults directly.

Supported backends:
- neo4j: Neo4j/Memgraph via Bolt protocol
- neptune: AWS Neptune via OpenCypher or Gremlin
- bigquery: Google Cloud Spanner Graph / BigQuery
- duckdb: Local DuckDB (query-only, reads Parquet directly)

Usage:
    backend = get_backend("neptune", config={...})
    await backend.publish(results)
    nodes = await backend.query_entities("3-NOP")
"""

from __future__ import annotations

import logging
import os
from typing import Any, Protocol, runtime_checkable

from ..connectors.base import ExtractionResult

logger = logging.getLogger(__name__)


@runtime_checkable
class GraphBackend(Protocol):
    """Protocol for graph database backends."""

    @property
    def name(self) -> str:
        """Backend identifier (neo4j, neptune, bigquery, duckdb)."""
        ...

    async def publish(
        self,
        results: list[ExtractionResult],
        **kwargs: Any,
    ) -> dict[str, int]:
        """Write extraction results to the graph. Returns counts."""
        ...

    async def query_entities(
        self,
        search_term: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Search for entities by name/text."""
        ...

    async def get_neighbors(
        self,
        entity_id: str,
        *,
        max_hops: int = 1,
    ) -> dict[str, Any]:
        """Get entity and its neighbors up to max_hops."""
        ...

    async def health_check(self) -> bool:
        """Return True if the backend is reachable."""
        ...


# ─── Registry ─────────────────────────────────────────────────────────────────


_BACKENDS: dict[str, type] = {}


def register_backend(name: str, cls: type) -> None:
    """Register a backend class by name."""
    _BACKENDS[name] = cls


def get_backend(name: str | None = None, **config: Any) -> GraphBackend:
    """Get a configured backend instance.

    If name is None, reads GRAPH_BACKEND env var (default: "neo4j").
    """
    backend_name = name or os.getenv("GRAPH_BACKEND", "neo4j")

    if backend_name not in _BACKENDS:
        # Lazy-load built-in backends
        _load_builtin(backend_name)

    if backend_name not in _BACKENDS:
        raise ValueError(
            f"Unknown backend: {backend_name}. "
            f"Available: {list(_BACKENDS.keys())}"
        )

    return _BACKENDS[backend_name](**config)


def _load_builtin(name: str) -> None:
    """Lazy-load a built-in backend to avoid import errors for missing deps."""
    if name == "neo4j":
        from .neo4j import Neo4jBackend

        register_backend("neo4j", Neo4jBackend)
    elif name == "neptune":
        from .neptune import NeptuneBackend

        register_backend("neptune", NeptuneBackend)
    elif name == "duckdb":
        from .duckdb import DuckDBBackend

        register_backend("duckdb", DuckDBBackend)
    elif name == "spanner":
        from .spanner import SpannerGraphBackend

        register_backend("spanner", SpannerGraphBackend)
    elif name == "falkordb":
        from .falkordb import FalkorDBBackend

        register_backend("falkordb", FalkorDBBackend)
