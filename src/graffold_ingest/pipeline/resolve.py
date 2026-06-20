"""Entity resolution — deduplicate and merge."""

from __future__ import annotations

from ..connectors.base import ExtractionResult


def resolve_entities(results: list[ExtractionResult]) -> list[ExtractionResult]:
    """Deduplicate entities across extraction results by name similarity.

    Merges nodes with the same name (case-insensitive) into a single entity.
    """
    # Build a canonical name → node mapping
    seen: dict[str, dict] = {}
    id_remap: dict[str, str] = {}

    merged_results: list[ExtractionResult] = []
    for result in results:
        resolved_nodes = []
        for node in result.nodes:
            name = node.get("name", "").lower().strip()
            if name in seen:
                # Remap this node's ID to the canonical one
                id_remap[node["id"]] = seen[name]["id"]
            else:
                seen[name] = node
                resolved_nodes.append(node)

        # Remap edge source/target IDs
        resolved_edges = []
        for edge in result.edges:
            edge = {**edge}
            edge["source"] = id_remap.get(edge["source"], edge["source"])
            edge["target"] = id_remap.get(edge["target"], edge["target"])
            resolved_edges.append(edge)

        merged_results.append(
            ExtractionResult(
                nodes=resolved_nodes,
                edges=resolved_edges,
                source_doc_id=result.source_doc_id,
            )
        )

    return merged_results
