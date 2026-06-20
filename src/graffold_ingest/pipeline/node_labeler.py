"""Node labeler — classifies generic Entity nodes into specific types.

Uses schema-aware heuristics and optional LLM classification to assign
proper labels to nodes that were extracted without a specific type.
"""

from __future__ import annotations

import logging
from typing import Any

from .schema import KGSchema

logger = logging.getLogger(__name__)


class NodeLabeler:
    """Labels generic Entity nodes with specific types from the schema."""

    def __init__(self, schema: KGSchema | None = None):
        self.schema = schema or KGSchema.load()
        self._build_patterns()

    def _build_patterns(self) -> None:
        """Build keyword lookup from schema examples and descriptions."""
        self._type_keywords: dict[str, list[str]] = {}
        for entity in self.schema.entities:
            keywords = [ex.lower() for ex in entity.examples]
            # Add words from description as soft signals
            keywords.extend(entity.description.lower().split())
            self._type_keywords[entity.name] = keywords

    def classify(self, name: str, current_type: str = "Entity") -> str:
        """Classify a node name into the best matching entity type.

        Returns the current_type unchanged if no better match is found.
        """
        if current_type != "Entity" and current_type in self.schema.entity_names:
            return current_type

        name_lower = name.lower().strip()

        # Exact match against examples
        for entity in self.schema.entities:
            if name_lower in (ex.lower() for ex in entity.examples):
                return entity.name

        # Keyword overlap scoring
        best_type = current_type
        best_score = 0
        for type_name, keywords in self._type_keywords.items():
            score = sum(1 for kw in keywords if kw in name_lower)
            if score > best_score:
                best_score = score
                best_type = type_name

        return best_type if best_score > 0 else current_type

    def label_nodes(self, nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add/update 'type' field on each node based on classification."""
        for node in nodes:
            current = node.get("type", "Entity")
            node["type"] = self.classify(node.get("name", ""), current)
        return nodes

    def label_in_graph(
        self,
        driver: Any,
        database: str = "neo4j",
    ) -> dict[str, int]:
        """Relabel generic Entity nodes in the graph database."""
        if not driver:
            return {}

        query = (
            "MATCH (n:Entity) WHERE n.type = 'Entity' OR n.type IS NULL "
            "RETURN n.id AS id, n.name AS name, n.type AS type LIMIT 1000"
        )
        with driver.session(database=database) as session:
            results = list(session.run(query))

        counts: dict[str, int] = {}
        for record in results:
            new_type = self.classify(record["name"] or "", record["type"] or "Entity")
            if new_type != "Entity":
                counts[new_type] = counts.get(new_type, 0) + 1

        # Apply labels in batch per type
        for label, _count in counts.items():
            ids = [
                r["id"] for r in results
                if self.classify(r["name"] or "", r["type"] or "Entity") == label
            ]
            if ids:
                with driver.session(database=database) as session:
                    session.run(
                        f"UNWIND $ids AS id MATCH (n:Entity {{id: id}}) SET n:{label}, n.type = $type",
                        {"ids": ids, "type": label},
                    )

        logger.info("Node labeling: %s", counts)
        return counts
