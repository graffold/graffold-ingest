"""SparseLink extraction — converts inferred edges to graph-ready nodes and relationships.

Unlike LLM extraction, this is deterministic: sparselink already produces
structured edge lists. We just convert them to the ExtractionResult format
with proper provenance.
"""

from __future__ import annotations

import json
from typing import Any

from ..connectors.base import Document, ExtractionResult


def extract_from_sparselink(documents: list[Document]) -> list[ExtractionResult]:
    """Convert sparselink inference results to graph-ready ExtractionResults.

    Each feature becomes a node, each inferred edge becomes a relationship.
    """
    results: list[ExtractionResult] = []

    for doc in documents:
        if doc.source_type != "sparselink":
            continue

        try:
            data = json.loads(doc.content)
        except json.JSONDecodeError:
            continue

        edges = data.get("edges", [])
        features = data.get("features", [])
        method = data.get("method", "unknown")

        # Create nodes for all features
        nodes: list[dict[str, Any]] = []
        seen_nodes: set[str] = set()

        for edge in edges:
            for name in (edge["source"], edge["target"]):
                if name not in seen_nodes:
                    nodes.append({
                        "id": name.lower().replace(" ", "_"),
                        "name": name,
                        "type": "Feature",
                        "label": "Feature",
                    })
                    seen_nodes.add(name)

        # Create edges
        graph_edges: list[dict[str, Any]] = []
        for edge in edges:
            src_id = edge["source"].lower().replace(" ", "_")
            tgt_id = edge["target"].lower().replace(" ", "_")
            weight = edge.get("weight", 0.0)

            graph_edges.append({
                "source_id": src_id,
                "target_id": tgt_id,
                "type": "INFERRED_LINK",
                "properties": {
                    "weight": weight,
                    "inference_method": method,
                    "source_sentence": f"Inferred by {method} (weight={weight:.4f})",
                },
            })

        results.append(
            ExtractionResult(
                nodes=nodes,
                edges=graph_edges,
                source_doc_id=doc.id,
            )
        )

    return results
