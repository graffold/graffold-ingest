"""Enhanced entity resolution with external resolver support."""

from __future__ import annotations

import logging
from typing import Any

from ..connectors.base import ExtractionResult
from .base import BaseResolver, ResolvedEntity
from .composite import CompositeResolver
from .mondo import MONDOResolver
from .pubchem import PubChemResolver
from .uniprot import UniProtResolver

logger = logging.getLogger(__name__)


def _default_resolvers() -> list[BaseResolver]:
    """Return the standard set of external resolvers."""
    return [UniProtResolver(), MONDOResolver(), PubChemResolver()]


def _dedup_pass(
    results: list[ExtractionResult],
) -> tuple[list[ExtractionResult], dict[str, str]]:
    """First pass: exact name dedup (existing logic).

    Returns resolved results and a mapping of canonical_name -> node_id.
    """
    seen: dict[str, dict[str, Any]] = {}
    id_remap: dict[str, str] = {}

    merged_results: list[ExtractionResult] = []
    for result in results:
        resolved_nodes: list[dict[str, Any]] = []
        for node in result.nodes:
            name = node.get("name", "").lower().strip()
            if name in seen:
                id_remap[node["id"]] = seen[name]["id"]
            else:
                seen[name] = node
                resolved_nodes.append(node)

        resolved_edges: list[dict[str, Any]] = []
        for edge in result.edges:
            edge = {**edge}
            src = edge.get("source", edge.get("source_id", ""))
            tgt = edge.get("target", edge.get("target_id", ""))
            edge["source"] = id_remap.get(src, src)
            edge["target"] = id_remap.get(tgt, tgt)
            resolved_edges.append(edge)

        merged_results.append(
            ExtractionResult(
                nodes=resolved_nodes,
                edges=resolved_edges,
                source_doc_id=result.source_doc_id,
            )
        )

    return merged_results, id_remap


async def _external_resolve_pass(
    results: list[ExtractionResult],
    composite: CompositeResolver,
) -> list[ExtractionResult]:
    """Second pass: external resolver lookup and merge by canonical ID.

    Entities that resolve to the same canonical_id get merged.
    """
    # Collect all nodes across results and resolve them
    canonical_map: dict[str, ResolvedEntity] = {}  # canonical_id -> resolved
    node_canonical: dict[str, str] = {}  # node_id -> canonical_id
    id_remap: dict[str, str] = {}  # duplicate node_id -> surviving node_id
    canonical_to_node_id: dict[str, str] = {}  # canonical_id -> first node_id

    for result in results:
        for node in result.nodes:
            name = node.get("name", "")
            label = node.get("label", "Entity")
            node_id = node["id"]

            resolved = await composite.resolve(name, label)
            if resolved is None:
                continue

            cid = resolved.canonical_id
            node_canonical[node_id] = cid

            if cid in canonical_map:
                # This entity resolves to one we've already seen — merge
                canonical_map[cid].source_names.append(name)
                id_remap[node_id] = canonical_to_node_id[cid]
            else:
                canonical_map[cid] = resolved
                canonical_to_node_id[cid] = node_id
                # Enrich node with canonical info
                node["canonical_id"] = cid
                node["canonical_name"] = resolved.canonical_name
                node["resolver"] = resolved.resolver
                node["resolver_confidence"] = resolved.confidence

    # Rebuild results with remapped edges and deduplicated nodes
    merged_results: list[ExtractionResult] = []
    for result in results:
        resolved_nodes = [n for n in result.nodes if n["id"] not in id_remap]

        resolved_edges: list[dict[str, Any]] = []
        for edge in result.edges:
            edge = {**edge}
            src = edge.get("source", edge.get("source_id", ""))
            tgt = edge.get("target", edge.get("target_id", ""))
            edge["source"] = id_remap.get(src, src)
            edge["target"] = id_remap.get(tgt, tgt)
            resolved_edges.append(edge)

        merged_results.append(
            ExtractionResult(
                nodes=resolved_nodes,
                edges=resolved_edges,
                source_doc_id=result.source_doc_id,
            )
        )

    return merged_results


async def resolve_entities_enhanced(
    results: list[ExtractionResult],
    resolvers: list[BaseResolver] | None = None,
    use_external: bool = True,
) -> list[ExtractionResult]:
    """Enhanced entity resolution.

    1. First pass: exact name dedup (existing logic)
    2. Second pass: external resolver lookup for canonical IDs
    3. Merge entities that resolved to the same canonical ID

    If use_external=False, only the name-dedup pass runs.
    """
    # Pass 1: exact name dedup
    deduped, _ = _dedup_pass(results)

    if not use_external:
        return deduped

    # Pass 2: external resolution
    resolver_list = resolvers if resolvers is not None else _default_resolvers()
    composite = CompositeResolver(resolver_list)
    resolved = await _external_resolve_pass(deduped, composite)

    return resolved
