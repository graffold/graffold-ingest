"""DRIFT search — Dynamic Reasoning and Inference with Flexible Traversal.

Multi-hop graph search that iteratively expands queries, retrieves context,
and synthesizes answers. Adapted from Microsoft GraphRAG (MIT License).
https://github.com/microsoft/graphrag

Algorithm:
1. Prime: Expand query into sub-questions
2. For each hop:
   a. Vector/text search for relevant entities
   b. Graph expand (1-hop neighbors)
   c. LLM evaluates: can we answer? Generate follow-ups?
3. Reduce: Synthesize final answer from intermediate answers
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# ─── Prompts (adapted from GraphRAG, MIT License) ─────────────────────────────
# Copyright (c) 2024 Microsoft Corporation

PRIME_PROMPT = """Given a research question, generate 2-4 focused sub-questions
that would help answer it comprehensively. Each sub-question should target a
different aspect of the original question.

Question: {query}

Return a JSON array of strings, e.g. ["sub-question 1", "sub-question 2"]
"""

EVALUATE_PROMPT = """You are analyzing whether the following context can answer
a research question.

Question: {query}

Context (entities and relationships from a knowledge graph):
{context}

Based on this context:
1. Can you provide a partial or complete answer? If yes, provide it.
2. What follow-up questions would help get a more complete answer?

Return JSON:
{{
  "can_answer": true/false,
  "answer": "your answer if can_answer is true, else empty string",
  "follow_ups": ["follow-up question 1", ...],
  "confidence": 0.0-1.0
}}
"""

REDUCE_PROMPT = """Synthesize a comprehensive answer from multiple partial
answers gathered during a multi-hop knowledge graph search.

Original question: {query}

Partial answers from different search paths:
{answers}

Provide a final, coherent answer that integrates all the information.
If there are contradictions, note them. Cite specific entities when relevant.
"""


@dataclass
class DriftState:
    """State maintained across DRIFT search hops."""

    query: str
    follow_ups: list[str] = field(default_factory=list)
    answers: list[str] = field(default_factory=list)
    entities_visited: set[str] = field(default_factory=set)
    hops: int = 0
    confidences: list[float] = field(default_factory=list)


@dataclass
class DriftResult:
    """Result of a DRIFT search."""

    answer: str
    hops: int
    entities_visited: list[str]
    intermediate_answers: list[str]
    follow_ups_explored: int
    confidence: float


async def drift_search(
    query: str,
    *,
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "neo4j",
    password: str = "",
    llm_service: str = "bedrock",
    llm_model: str = "",
    max_hops: int = 5,
    k_per_hop: int = 10,
) -> DriftResult:
    """Execute DRIFT search against the knowledge graph.

    Args:
        query: The research question to answer.
        database_uri: Neo4j bolt URI.
        database_name: Database name.
        username/password: Auth.
        llm_service: LLM backend for reasoning.
        llm_model: Model ID override.
        max_hops: Maximum search iterations.
        k_per_hop: Entities to retrieve per hop.

    Returns:
        DriftResult with final answer and search metadata.
    """
    from .extract import _call_llm

    state = DriftState(query=query)

    # ─── Phase 1: Prime ────────────────────────────────────────────────
    prime_prompt = PRIME_PROMPT.format(query=query)
    try:
        raw = await _call_llm(prime_prompt, llm_service, llm_model)
        sub_questions = json.loads(raw)
        if isinstance(sub_questions, list):
            state.follow_ups = sub_questions[:4]
        else:
            state.follow_ups = [query]
    except Exception:
        state.follow_ups = [query]

    logger.info("DRIFT prime: %d sub-questions for '%s'", len(state.follow_ups), query[:60])

    # ─── Phase 2: Iterative search ────────────────────────────────────
    follow_ups_explored = 0

    while state.hops < max_hops and state.follow_ups:
        current_query = state.follow_ups.pop(0)
        follow_ups_explored += 1

        # Local search: find entities matching the current query
        context = await _local_entity_search(
            current_query,
            database_uri=database_uri,
            database_name=database_name,
            username=username,
            password=password,
            limit=k_per_hop,
            exclude_ids=state.entities_visited,
        )

        if not context["entities"]:
            state.hops += 1
            continue

        state.entities_visited.update(context["entity_ids"])

        # LLM evaluation
        context_text = _format_context(context)
        eval_prompt = EVALUATE_PROMPT.format(query=current_query, context=context_text)

        try:
            raw = await _call_llm(eval_prompt, llm_service, llm_model)
            eval_result = json.loads(raw)
        except Exception:
            eval_result = {"can_answer": False, "answer": "", "follow_ups": [], "confidence": 0}

        if eval_result.get("can_answer"):
            state.answers.append(eval_result["answer"])
            state.confidences.append(eval_result.get("confidence", 0.5))
        
        # Add new follow-ups (limit to prevent explosion)
        new_follow_ups = eval_result.get("follow_ups", [])
        state.follow_ups.extend(new_follow_ups[:2])

        state.hops += 1

    # ─── Phase 3: Reduce ───────────────────────────────────────────────
    if not state.answers:
        return DriftResult(
            answer="Insufficient information in the knowledge graph to answer this question.",
            hops=state.hops,
            entities_visited=list(state.entities_visited),
            intermediate_answers=[],
            follow_ups_explored=follow_ups_explored,
            confidence=0.0,
        )

    if len(state.answers) == 1:
        final_answer = state.answers[0]
    else:
        answers_text = "\n\n".join(
            f"[Path {i+1}]: {a}" for i, a in enumerate(state.answers)
        )
        reduce_prompt = REDUCE_PROMPT.format(query=query, answers=answers_text)
        try:
            final_answer = await _call_llm(reduce_prompt, llm_service, llm_model)
        except Exception:
            final_answer = state.answers[0]

    avg_confidence = (
        sum(state.confidences) / len(state.confidences) if state.confidences else 0.0
    )

    logger.info(
        "DRIFT complete: %d hops, %d entities, %d answers, confidence=%.2f",
        state.hops, len(state.entities_visited), len(state.answers), avg_confidence,
    )

    return DriftResult(
        answer=final_answer,
        hops=state.hops,
        entities_visited=list(state.entities_visited),
        intermediate_answers=state.answers,
        follow_ups_explored=follow_ups_explored,
        confidence=avg_confidence,
    )


# ─── Helpers ───────────────────────────────────────────────────────────────────


async def _local_entity_search(
    query: str,
    *,
    database_uri: str,
    database_name: str,
    username: str,
    password: str,
    limit: int = 10,
    exclude_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Search for entities matching query and expand 1-hop neighbors."""
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(database_uri, auth=(username, password))
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    entity_ids: set[str] = set()

    try:
        async with driver.session(database=database_name) as session:
            # Text search for matching entities
            search_query = """
                MATCH (e)
                WHERE e.name IS NOT NULL
                  AND toLower(e.name) CONTAINS toLower($term)
                WITH e LIMIT $limit
                OPTIONAL MATCH (e)-[r]-(neighbor)
                RETURN e.id AS id, e.name AS name, labels(e) AS labels,
                       e.description AS description,
                       type(r) AS rel_type, r AS rel_props,
                       neighbor.id AS neighbor_id, neighbor.name AS neighbor_name,
                       labels(neighbor) AS neighbor_labels
            """
            # Extract key terms from query for search
            terms = _extract_search_terms(query)

            for term in terms[:3]:
                result = await session.run(
                    search_query, {"term": term, "limit": limit}
                )
                records = await result.data()

                for record in records:
                    eid = record.get("id", "")
                    if exclude_ids and eid in exclude_ids:
                        continue
                    if eid and eid not in entity_ids:
                        entity_ids.add(eid)
                        entities.append({
                            "id": eid,
                            "name": record.get("name", ""),
                            "type": (record.get("labels") or ["Entity"])[0],
                            "description": record.get("description", ""),
                        })
                    # Collect relationships
                    if record.get("rel_type") and record.get("neighbor_name"):
                        relationships.append({
                            "source": record.get("name", ""),
                            "target": record.get("neighbor_name", ""),
                            "type": record["rel_type"],
                        })
    finally:
        await driver.close()

    return {
        "entities": entities,
        "relationships": relationships,
        "entity_ids": entity_ids,
    }


def _extract_search_terms(text: str) -> list[str]:
    """Extract meaningful search terms from a query."""
    import re

    stopwords = {
        "what", "which", "how", "why", "when", "where", "who", "the", "a", "an",
        "is", "are", "was", "were", "be", "been", "have", "has", "had", "do",
        "does", "did", "will", "would", "could", "should", "may", "might",
        "for", "of", "to", "in", "on", "at", "by", "with", "from", "and", "or",
        "but", "not", "this", "that", "these", "those", "can", "about", "into",
    }
    words = re.findall(r"\b[a-zA-Z0-9\-]{3,}\b", text)
    return [w for w in words if w.lower() not in stopwords][:10]


def _format_context(context: dict[str, Any]) -> str:
    """Format search context for LLM consumption."""
    lines = ["Entities found:"]
    for e in context["entities"][:15]:
        desc = f" — {e['description']}" if e.get("description") else ""
        lines.append(f"  • {e['name']} ({e['type']}){desc}")

    if context["relationships"]:
        lines.append("\nRelationships:")
        seen = set()
        for r in context["relationships"][:20]:
            key = (r["source"], r["type"], r["target"])
            if key not in seen:
                seen.add(key)
                lines.append(f"  • {r['source']} —[{r['type']}]→ {r['target']}")

    return "\n".join(lines)
