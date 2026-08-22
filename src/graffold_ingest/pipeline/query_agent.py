"""Query agent — deterministic 5-phase graph-grounded QA.

Architecture adapted from graffold-api's TwoPhaseAgent, made backend-agnostic:

  Phase 1: Discovery (entity search + vector lookup)
  Phase 2: Expansion (neighbor traversal, 1-2 hops)
  Phase 3: Relevance filtering (drop noise before LLM)
  Phase 4: LLM synthesis (reason over graph context)
  Phase 5: Fact verification (check claims against graph)

Works with any GraphBackend (Neptune, Spanner, Neo4j, DuckDB).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from ..backends import GraphBackend, get_backend
from ..pipeline.extract import _call_llm

logger = logging.getLogger(__name__)

SYNTHESIS_PROMPT = """You are a biomedical knowledge graph assistant. Answer the question using ONLY the graph context below. If the context doesn't contain enough information, say so.

## Graph Context

### Entities Found
{entities_text}

### Relationships
{relationships_text}

### Neighbor Context
{neighbors_text}

## Question
{question}

## Instructions
- Cite specific entities and relationships from the context
- If evidence is contradictory, note both sides
- Be concise and precise
- If you cannot answer from the context, say "Insufficient graph coverage"

Answer:"""

VERIFY_PROMPT = """Given this answer and the graph entities/relationships that support it, identify any claims NOT grounded in the provided graph data.

Answer: {answer}

Graph entities: {entities}
Graph relationships: {relationships}

Return a JSON list of ungrounded claims (empty list if all claims are grounded):
["claim 1 not in graph", "claim 2 not in graph"]

Return ONLY valid JSON:"""


@dataclass
class QueryResult:
    """Result of a graph-grounded query."""

    answer: str
    entities: list[dict[str, Any]] = field(default_factory=list)
    relationships: list[dict[str, Any]] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    ungrounded_claims: list[str] = field(default_factory=list)
    phases: dict[str, float] = field(default_factory=dict)  # phase → seconds
    total_seconds: float = 0.0


async def query_graph(
    question: str,
    *,
    backend: GraphBackend | None = None,
    backend_name: str | None = None,
    llm_service: str = "bedrock",
    llm_model: str = "",
    max_hops: int = 1,
    max_entities: int = 20,
    verify: bool = True,
) -> QueryResult:
    """Run a 5-phase graph-grounded query.

    Args:
        question: Natural language question
        backend: GraphBackend instance (or auto-resolve from env)
        backend_name: Backend name override (neptune, spanner, neo4j, duckdb)
        llm_service: LLM service for synthesis (bedrock, openai, ollama)
        llm_model: Model ID override
        max_hops: Neighbor expansion depth
        max_entities: Max entities to retrieve in discovery
        verify: Run Phase 5 fact verification

    Returns:
        QueryResult with answer, entities, relationships, timing
    """
    t0 = time.time()
    phases: dict[str, float] = {}

    if backend is None:
        backend = get_backend(backend_name)

    # ─── Phase 1: Discovery ────────────────────────────────────────────
    t1 = time.time()
    # Extract search terms from question
    terms = _extract_search_terms(question)
    entities: list[dict[str, Any]] = []

    for term in terms[:5]:  # Cap at 5 searches
        found = await backend.query_entities(term, limit=max_entities)
        entities.extend(found)

    # Deduplicate by ID
    seen_ids: set[str] = set()
    unique_entities: list[dict[str, Any]] = []
    for e in entities:
        eid = e.get("id", "")
        if eid and eid not in seen_ids:
            seen_ids.add(eid)
            unique_entities.append(e)
    entities = unique_entities[:max_entities]

    phases["discovery"] = time.time() - t1
    logger.info("Phase 1: found %d entities for %d terms", len(entities), len(terms))

    # ─── Phase 2: Expansion ────────────────────────────────────────────
    t2 = time.time()
    neighbors_context: list[dict[str, Any]] = []

    for entity in entities[:10]:  # Expand top 10
        eid = entity.get("id", "")
        if eid:
            result = await backend.get_neighbors(eid, max_hops=max_hops)
            if result.get("neighbors"):
                neighbors_context.append({
                    "source": entity.get("name", eid),
                    "neighbors": result["neighbors"],
                })

    phases["expansion"] = time.time() - t2
    logger.info("Phase 2: expanded %d entities, found %d neighbor sets",
                min(len(entities), 10), len(neighbors_context))

    # ─── Phase 3: Relevance filtering ─────────────────────────────────
    t3 = time.time()
    # Simple keyword relevance — keep entities whose name appears related to question
    q_lower = question.lower()
    scored = []
    for e in entities:
        name = e.get("name", "").lower()
        score = 1.0 if any(t in name for t in terms) else 0.3
        scored.append((score, e))
    scored.sort(key=lambda x: x[0], reverse=True)
    entities = [e for _, e in scored[:max_entities]]

    phases["filtering"] = time.time() - t3

    # ─── Phase 4: LLM Synthesis ───────────────────────────────────────
    t4 = time.time()

    entities_text = "\n".join(
        f"- {e.get('name', e.get('id', '?'))} ({e.get('type', e.get('labels', '?'))})"
        for e in entities
    ) or "None found"

    relationships_text = "\n".join(
        f"- {n['source']} → {', '.join(nb.get('name', '?') for nb in n['neighbors'][:5])}"
        for n in neighbors_context
    ) or "None found"

    neighbors_text = "\n".join(
        f"- {n['source']} connects to: {', '.join(nb.get('name', '?') for nb in n['neighbors'][:5])}"
        for n in neighbors_context[:5]
    ) or "No expansion performed"

    prompt = SYNTHESIS_PROMPT.format(
        entities_text=entities_text,
        relationships_text=relationships_text,
        neighbors_text=neighbors_text,
        question=question,
    )

    try:
        answer = await _call_llm(prompt, llm_service, llm_model)
    except Exception as e:
        logger.warning("LLM synthesis failed: %s", e)
        answer = f"Graph contains {len(entities)} relevant entities but LLM synthesis failed."

    phases["synthesis"] = time.time() - t4
    logger.info("Phase 4: synthesized answer (%d chars)", len(answer))

    # ─── Phase 5: Fact verification ───────────────────────────────────
    ungrounded: list[str] = []
    if verify and entities:
        t5 = time.time()
        verify_prompt = VERIFY_PROMPT.format(
            answer=answer[:2000],
            entities=entities_text[:2000],
            relationships=relationships_text[:2000],
        )
        try:
            import json
            raw = await _call_llm(verify_prompt, llm_service, llm_model)
            ungrounded = json.loads(raw)
            if not isinstance(ungrounded, list):
                ungrounded = []
        except Exception:
            ungrounded = []
        phases["verification"] = time.time() - t5

    total = time.time() - t0
    logger.info("Query complete: %.1fs total, %d entities, %d ungrounded claims",
                total, len(entities), len(ungrounded))

    return QueryResult(
        answer=answer,
        entities=entities,
        relationships=[
            {"source": n["source"], "targets": [nb.get("name") for nb in n["neighbors"][:5]]}
            for n in neighbors_context
        ],
        sources=[e.get("id", "") for e in entities if e.get("id")],
        ungrounded_claims=ungrounded,
        phases=phases,
        total_seconds=total,
    )


def _extract_search_terms(question: str) -> list[str]:
    """Extract likely entity names from a question (simple heuristic)."""
    import re

    # Remove common question words
    stopwords = {
        "what", "which", "how", "does", "do", "is", "are", "the", "a", "an",
        "of", "in", "to", "and", "or", "for", "with", "that", "this", "from",
        "by", "on", "it", "be", "was", "were", "been", "being", "have", "has",
        "had", "can", "could", "would", "should", "will", "may", "might",
        "about", "between", "through", "during", "before", "after",
    }
    # Split on non-alphanumeric, keep tokens ≥ 2 chars
    tokens = re.findall(r"[A-Za-z0-9][\w\-]*", question)
    terms = [t for t in tokens if t.lower() not in stopwords and len(t) >= 2]
    # Also try the full question as a phrase search
    if len(terms) > 1:
        terms.append(question.strip()[:50])
    return terms
