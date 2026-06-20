"""Schema discovery — generates a domain schema YAML from sample data.

Given sample documents or a description of the domain, uses an LLM to
propose entity types, relationship types, examples, and extraction rules.

Usage:
    graffold-ingest schema discover --from-file sample.pdf
    graffold-ingest schema discover --from-url "https://..."
    graffold-ingest schema discover --domain "cybersecurity threat intelligence"
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

DISCOVER_PROMPT = """You are a knowledge graph schema designer. Given the following content (or domain description), design a schema YAML for entity and relationship extraction.

Return ONLY valid YAML with this structure:

```yaml
version: "1.0"
description: "<one-line description of this domain's knowledge graph>"

entities:
  - name: <EntityType>
    description: "<what this entity represents>"
    examples: ["<3-5 real examples from the content>"]
    anti_examples: ["<generic terms that should NOT be extracted>"]

relationships:
  - type: <RELATIONSHIP_TYPE>
    description: "<what this relationship means>"
    source: [<valid source entity types>]
    target: [<valid target entity types>]

extraction_rules:
  - "<rule 1>"
  - "<rule 2>"
```

Design 4-8 entity types and 5-12 relationship types that capture the key knowledge in this domain.
Focus on SPECIFIC named entities, not generic concepts.

Content/Domain:
{content}
"""

REFINE_PROMPT = """Here is an existing knowledge graph schema:

```yaml
{schema}
```

The user wants to refine it:
{feedback}

Return the complete updated YAML schema (not a diff). Preserve the structure.
Return ONLY valid YAML.
"""


async def discover_schema(
    content: str = "",
    domain: str = "",
    llm_service: str = "bedrock",
    model_id: str = "",
) -> str:
    """Generate a schema YAML from content or domain description.

    Args:
        content: Sample text to analyze (document content, URL text, etc.)
        domain: Plain-English domain description if no content provided
        llm_service: LLM backend
        model_id: Model override

    Returns:
        YAML string ready to save to a file.
    """
    from .extract import _call_llm

    input_text = content[:6000] if content else f"Domain: {domain}"
    prompt = DISCOVER_PROMPT.format(content=input_text)

    response = await _call_llm(prompt, llm_service, model_id or "")
    return _extract_yaml(response)


async def refine_schema(
    current_schema: str,
    feedback: str,
    llm_service: str = "bedrock",
    model_id: str = "",
) -> str:
    """Refine an existing schema based on user feedback.

    Args:
        current_schema: Current YAML schema content
        feedback: What to change (e.g., "add a Technology entity type")
        llm_service: LLM backend

    Returns:
        Updated YAML string.
    """
    from .extract import _call_llm

    prompt = REFINE_PROMPT.format(schema=current_schema, feedback=feedback)
    response = await _call_llm(prompt, llm_service, model_id or "")
    return _extract_yaml(response)


def validate_schema(yaml_content: str) -> list[str]:
    """Validate a schema YAML and return a list of issues (empty = valid)."""
    issues: list[str] = []
    try:
        data = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return [f"Invalid YAML: {e}"]

    if not isinstance(data, dict):
        return ["Schema must be a YAML mapping"]

    if "entities" not in data:
        issues.append("Missing 'entities' section")
    elif not isinstance(data["entities"], list):
        issues.append("'entities' must be a list")
    else:
        for i, e in enumerate(data["entities"]):
            if "name" not in e:
                issues.append(f"Entity {i}: missing 'name'")
            if "examples" not in e or not e["examples"]:
                issues.append(f"Entity '{e.get('name', i)}': needs at least one example")

    if "relationships" not in data:
        issues.append("Missing 'relationships' section")
    elif not isinstance(data["relationships"], list):
        issues.append("'relationships' must be a list")
    else:
        entity_names = {e.get("name") for e in data.get("entities", [])}
        for i, r in enumerate(data["relationships"]):
            if "type" not in r:
                issues.append(f"Relationship {i}: missing 'type'")
            for src in r.get("source", []):
                if src not in entity_names:
                    issues.append(f"Relationship '{r.get('type', i)}': source '{src}' not in entities")
            for tgt in r.get("target", []):
                if tgt not in entity_names:
                    issues.append(f"Relationship '{r.get('type', i)}': target '{tgt}' not in entities")

    return issues


def save_schema(yaml_content: str, path: str | Path) -> Path:
    """Save schema YAML to file after validation."""
    issues = validate_schema(yaml_content)
    if issues:
        logger.warning("Schema has issues: %s", issues)

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml_content)
    logger.info("Schema saved to %s", path)
    return path


def _extract_yaml(response: str) -> str:
    """Extract YAML from LLM response, stripping markdown fences."""
    import re

    # Try to find YAML block in markdown fences
    match = re.search(r"```(?:yaml)?\s*\n(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()
    # If no fences, assume the whole response is YAML
    return response.strip()
