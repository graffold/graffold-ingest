"""KG Schema loader — reads schema YAML and generates LLM extraction prompts.

Users define their domain schema in YAML. The schema:
1. Guides the LLM to extract the right entity/relationship types
2. Validates extracted results (rejects anti-examples, generic terms)
3. Documents the knowledge graph structure

Usage:
    from graffold_ingest.pipeline.schema import KGSchema

    schema = KGSchema.load("my_domain.yaml")
    prompt = schema.build_extraction_prompt(text="...")
    valid_nodes = schema.validate_entities(extracted_nodes)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_PATH = Path(__file__).parent / "default_schema.yaml"


@dataclass
class EntityType:
    name: str
    description: str = ""
    identifiers: list[str] = field(default_factory=list)
    examples: list[str] = field(default_factory=list)
    anti_examples: list[str] = field(default_factory=list)


@dataclass
class RelationshipType:
    type: str
    description: str = ""
    source: list[str] = field(default_factory=list)
    target: list[str] = field(default_factory=list)


@dataclass
class KGSchema:
    version: str
    description: str
    entities: list[EntityType]
    relationships: list[RelationshipType]
    extraction_rules: list[str]

    @classmethod
    def load(cls, path: Path | str | None = None) -> "KGSchema":
        """Load schema from YAML file."""
        path = Path(path) if path else DEFAULT_SCHEMA_PATH
        if not path.exists():
            raise FileNotFoundError(f"Schema not found: {path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        entities = [EntityType(**e) for e in data.get("entities", [])]
        relationships = [RelationshipType(**r) for r in data.get("relationships", [])]

        return cls(
            version=data.get("version", "unknown"),
            description=data.get("description", ""),
            entities=entities,
            relationships=relationships,
            extraction_rules=data.get("extraction_rules", []),
        )

    @property
    def entity_names(self) -> list[str]:
        return [e.name for e in self.entities]

    @property
    def relationship_type_names(self) -> list[str]:
        return [r.type for r in self.relationships]

    def build_extraction_prompt(self, text: str) -> str:
        """Generate the full LLM extraction prompt from schema + input text."""
        entity_section = "\n".join(
            f"  - {e.name}: {e.description}"
            + (f" (e.g., {', '.join(e.examples[:3])})" if e.examples else "")
            for e in self.entities
        )

        anti_examples: list[str] = []
        for e in self.entities:
            anti_examples.extend(e.anti_examples)
        anti_section = ", ".join(sorted(set(anti_examples))) or "none"

        rel_section = "\n".join(
            f"  - {r.type}: {r.description}"
            + (f" ({' | '.join(r.source)} → {' | '.join(r.target)})" if r.source else "")
            for r in self.relationships
        )

        rules_section = "\n".join(f"  - {rule}" for rule in self.extraction_rules)

        return f"""Extract entities and relationships from the following text. Return a JSON object:

{{
    "nodes": [
        {{"id": "unique_id", "name": "entity_name", "type": "entity_type"}}
    ],
    "relationships": [
        {{"source_id": "source_entity_id", "target_id": "target_entity_id", "type": "relationship_type", "source_sentence": "verbatim sentence from text"}}
    ]
}}

Entity types:
{entity_section}

Relationship types:
{rel_section}

Extraction rules:
{rules_section}

NEVER extract these generic terms as entities: {anti_section}

Text:
{text}

Return only valid JSON:"""

    def validate_entities(self, nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter out invalid entities based on schema rules."""
        all_anti = set()
        for e in self.entities:
            all_anti.update(word.lower() for word in e.anti_examples)

        valid_types = set(self.entity_names)
        valid: list[dict[str, Any]] = []

        for node in nodes:
            name = node.get("name", "").strip()
            if not name or len(name) < 2:
                continue
            if name.lower() in all_anti:
                continue
            if node.get("type", "") not in valid_types:
                node["type"] = "Entity"
            valid.append(node)

        rejected = len(nodes) - len(valid)
        if rejected:
            logger.info(f"Schema validation: kept {len(valid)}/{len(nodes)} ({rejected} rejected)")
        return valid

    def validate_relationships(
        self, rels: list[dict[str, Any]], nodes: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Filter relationships: both endpoints must exist, must have source_sentence."""
        node_ids = {n["id"] for n in nodes}
        return [
            r for r in rels
            if r.get("source_id") in node_ids
            and r.get("target_id") in node_ids
            and r.get("source_sentence")
        ]
