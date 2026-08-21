"""Global search — map-reduce over community summaries.

Answers broad questions by consulting all community summaries in parallel,
then synthesizing into a single coherent answer.

Adapted from Microsoft GraphRAG (MIT License).
https://github.com/microsoft/graphrag
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# ─── Prompts (adapted from GraphRAG, MIT License) ─────────────────────────────
# Copyright (c) 2024 Microsoft Corporation

MAP_PROMPT = """You are an analyst examining a specific segment of a knowledge graph.

Research question: {query}

Community summary (a cluster of related entities):
Title: {title}
{summary}

Rate how relevant this community is to the research question (0-10).
If relevant (score > 3), provide key points that help answer the question.

Return JSON:
{{
  "score": 0-10,
  "key_points": ["point 1", "point 2", ...],
  "entities_mentioned": ["entity name 1", ...]
}}
"""

REDUCE_PROMPT = """You are synthesizing research findings from multiple knowledge
graph communities to answer a question comprehensively.

Research question: {query}

Findings from relevant communities:
{findings}

Synthesize a comprehensive answer. Include:
1. A direct answer to the question
2. Supporting evidence from the communities
3. Any contradictions or gaps in the evidence
4. Confidence level (low/medium/high)

Be specific and cite entity names when relevant.
"""


@dataclass
class GlobalSearchResult:
    """Result of a global search."""

    answer: str
    communities_consulted: int
    communities_relevant: int
    key_points: list[str] = field(default_factory=list)
    entities_mentioned: list[str] = field(default_factory=list)
    confidence: str = "low"


async def global_search(
    query: str,
    *,
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "neo4j",
    password: str = "",
    llm_service: str = "bedrock",
    llm_model: str = "",
    max_concurrent: int = 5,
    min_relevance_score: int = 3,
    parquet_dir: str | None = None,
) -> GlobalSearchResult:
    """Execute global search over community summaries.

    Requires communities to have been detected and summarized first
    (via detect_communities + summarize_communities, or loaded from Parquet).

    Args:
        query: The broad research question.
        database_uri: Neo4j bolt URI (used if parquet_dir is None).
        database_name: Database name.
        username/password: Auth.
        llm_service: LLM backend for map/reduce.
        llm_model: Model ID override.
        max_concurrent: Max parallel LLM calls in map phase.
        min_relevance_score: Minimum score (0-10) to include in reduce.
        parquet_dir: If set, load communities from Parquet instead of Neo4j.

    Returns:
        GlobalSearchResult with synthesized answer.
    """
    from .extract import _call_llm

    # ─── Load communities ──────────────────────────────────────────────
    if parquet_dir:
        communities = _load_communities_from_parquet(parquet_dir)
    else:
        communities = await _load_communities_from_neo4j(
            database_uri, database_name, username, password
        )

    if not communities:
        return GlobalSearchResult(
            answer="No community summaries available. Run community detection first.",
            communities_consulted=0,
            communities_relevant=0,
            confidence="low",
        )

    logger.info("Global search: consulting %d communities for '%s'", len(communities), query[:60])

    # ─── Map phase ─────────────────────────────────────────────────────
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _map_one(community: dict) -> dict[str, Any]:
        async with semaphore:
            prompt = MAP_PROMPT.format(
                query=query,
                title=community.get("title", "Untitled"),
                summary=community.get("summary", "No summary available."),
            )
            try:
                raw = await _call_llm(prompt, llm_service, llm_model)
                return json.loads(raw)
            except Exception:
                return {"score": 0, "key_points": [], "entities_mentioned": []}

    map_results = await asyncio.gather(*[_map_one(c) for c in communities])

    # ─── Filter relevant ───────────────────────────────────────────────
    relevant = [
        r for r in map_results if r.get("score", 0) > min_relevance_score
    ]

    if not relevant:
        return GlobalSearchResult(
            answer="The knowledge graph communities do not contain sufficient information to answer this question.",
            communities_consulted=len(communities),
            communities_relevant=0,
            confidence="low",
        )

    # ─── Reduce phase ──────────────────────────────────────────────────
    findings_text = "\n\n".join(
        f"[Community {i+1}] (relevance: {r['score']}/10)\n"
        f"Key points: {', '.join(r.get('key_points', []))}"
        for i, r in enumerate(relevant)
    )

    reduce_prompt = REDUCE_PROMPT.format(query=query, findings=findings_text)

    try:
        final_answer = await _call_llm(reduce_prompt, llm_service, llm_model)
    except Exception as e:
        logger.warning("Global search reduce failed: %s", e)
        # Fallback: concatenate key points
        all_points = [p for r in relevant for p in r.get("key_points", [])]
        final_answer = "Key findings:\n" + "\n".join(f"• {p}" for p in all_points)

    # Aggregate metadata
    all_points = [p for r in relevant for p in r.get("key_points", [])]
    all_entities = list({
        e for r in relevant for e in r.get("entities_mentioned", [])
    })

    # Confidence based on coverage
    coverage_ratio = len(relevant) / len(communities) if communities else 0
    if coverage_ratio > 0.3:
        confidence = "high"
    elif coverage_ratio > 0.1:
        confidence = "medium"
    else:
        confidence = "low"

    logger.info(
        "Global search complete: %d/%d communities relevant, confidence=%s",
        len(relevant), len(communities), confidence,
    )

    return GlobalSearchResult(
        answer=final_answer,
        communities_consulted=len(communities),
        communities_relevant=len(relevant),
        key_points=all_points,
        entities_mentioned=all_entities,
        confidence=confidence,
    )


# ─── Helpers ───────────────────────────────────────────────────────────────────


async def _load_communities_from_neo4j(
    database_uri: str,
    database_name: str,
    username: str,
    password: str,
) -> list[dict[str, Any]]:
    """Load community summaries from Neo4j."""
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(database_uri, auth=(username, password))
    communities: list[dict[str, Any]] = []

    try:
        async with driver.session(database=database_name) as session:
            result = await session.run("""
                MATCH (c:Community)
                WHERE c.summary IS NOT NULL AND c.summary <> ''
                RETURN c.id AS id, c.title AS title, c.summary AS summary,
                       c.level AS level, c.size AS size
                ORDER BY c.size DESC
            """)
            records = await result.data()
            communities = [dict(r) for r in records]
    except Exception as e:
        logger.warning("Failed to load communities from Neo4j: %s", e)
    finally:
        await driver.close()

    return communities


def _load_communities_from_parquet(parquet_dir: str) -> list[dict[str, Any]]:
    """Load community summaries from Parquet file."""
    from pathlib import Path

    import pyarrow.parquet as pq

    path = Path(parquet_dir) / "communities.parquet"
    if not path.exists():
        return []

    table = pq.read_table(path)
    records = table.to_pylist()
    return [r for r in records if r.get("summary")]
