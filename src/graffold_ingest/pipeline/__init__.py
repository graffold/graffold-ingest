"""Ingestion pipeline stages."""

from .chunk import chunk_documents
from .community import (
    Community,
    CommunityResult,
    detect_communities,
    summarize_communities,
)
from .dedup import Deduplicator, content_hash
from .drift_search import DriftResult, drift_search
from .dual_write import publish_dual
from .global_search import GlobalSearchResult, global_search
from .entity_push import EntityPushStats, process_entity_push
from .extract import extract_entities
from .orchestrator import PipelineResult, run_pipeline
from .publish import publish_to_graph
from .publish_parquet import parquet_stats, publish_to_parquet, read_parquet_graph
from .resolve import resolve_entities
from .schema import KGSchema
from .sparselink_extract import extract_from_sparselink

__all__ = [
    "Community",
    "CommunityResult",
    "Deduplicator",
    "DriftResult",
    "EntityPushStats",
    "GlobalSearchResult",
    "KGSchema",
    "PipelineResult",
    "chunk_documents",
    "content_hash",
    "detect_communities",
    "drift_search",
    "extract_entities",
    "global_search",
    "extract_from_sparselink",
    "parquet_stats",
    "process_entity_push",
    "publish_dual",
    "publish_to_graph",
    "publish_to_parquet",
    "read_parquet_graph",
    "resolve_entities",
    "run_pipeline",
    "summarize_communities",
]
