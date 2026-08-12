"""Leiden community detection — hierarchical graph clustering.

Uses graspologic-native for Leiden algorithm. Produces multi-level
community assignments compatible with GraphRAG-style search.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

try:
    from graspologic_native import leiden
except ImportError:
    leiden = None  # type: ignore[assignment]
    logger.warning("graspologic-native not installed — community detection unavailable")

# Default resolutions: fine → coarse
_DEFAULT_RESOLUTIONS: list[float] = [1.0, 0.5, 0.25]


@dataclass
class Community:
    """A detected community at a specific hierarchy level."""

    id: str  # Unique community ID
    level: int  # Hierarchy level (0 = finest)
    title: str = ""  # Generated summary title
    summary: str = ""  # Generated summary text
    member_ids: list[str] = field(default_factory=list)  # Node IDs in community
    parent_id: str | None = None  # Parent community at level+1
    size: int = 0  # Number of members


@dataclass
class CommunityResult:
    """Result of community detection."""

    communities: list[Community]
    node_assignments: dict[str, list[str]]  # node_id -> [community_ids per level]
    num_levels: int
    modularity_scores: list[float] = field(default_factory=list)


def detect_communities(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    *,
    resolutions: list[float] | None = None,
    seed: int = 42,
) -> CommunityResult:
    """Run hierarchical Leiden community detection.

    Args:
        nodes: List of node dicts with 'id' key.
        edges: List of edge dicts with 'source'/'source_id' and
               'target'/'target_id' keys.
        resolutions: Leiden resolution parameters for each level.
                     Default: [1.0, 0.5, 0.25] (fine → coarse).
        seed: Random seed for reproducibility.

    Returns:
        CommunityResult with hierarchical community assignments.

    Raises:
        RuntimeError: If graspologic-native is not installed.
    """
    if leiden is None:
        raise RuntimeError(
            "graspologic is not installed. Install with: pip install graspologic-native"
        )

    if not nodes:
        return CommunityResult(communities=[], node_assignments={}, num_levels=0)

    resolutions = resolutions or _DEFAULT_RESOLUTIONS

    # Collect node IDs (needed for isolated nodes)
    node_ids = [n["id"] for n in nodes]
    node_id_set = set(node_ids)

    # Build edge list as List[Tuple[str, str, float]]
    edge_tuples: list[tuple[str, str, float]] = []
    for edge in edges:
        source = edge.get("source") or edge.get("source_id")
        target = edge.get("target") or edge.get("target_id")
        if source in node_id_set and target in node_id_set:
            weight = float(edge.get("weight", 1.0))
            edge_tuples.append((source, target, weight))

    # Handle graphs with no edges — every node is its own community
    if not edge_tuples:
        return _singleton_communities(node_ids, len(resolutions))

    # Run Leiden at each resolution level
    all_communities: list[Community] = []
    node_assignments: dict[str, list[str]] = {nid: [] for nid in node_ids}
    modularity_scores: list[float] = []
    level_partitions: list[dict[int, Community]] = []

    for level, resolution in enumerate(resolutions):
        quality, partition = leiden(edge_tuples, resolution=resolution, seed=seed)
        modularity_scores.append(quality)

        # Group nodes by community label
        label_to_members: dict[int, list[str]] = {}
        for node_id, label in partition.items():
            label_to_members.setdefault(label, []).append(node_id)

        # Assign isolated nodes (not in any edge) to singleton communities
        partitioned_nodes = set(partition.keys())
        isolated = [nid for nid in node_ids if nid not in partitioned_nodes]
        next_label = max(label_to_members.keys(), default=-1) + 1
        for iso_node in isolated:
            label_to_members[next_label] = [iso_node]
            next_label += 1

        # Build Community objects for this level
        label_to_community: dict[int, Community] = {}
        for lbl, members in label_to_members.items():
            community = Community(
                id=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"c-{level}-{lbl}-{seed}")),
                level=level,
                member_ids=members,
                size=len(members),
            )
            label_to_community[lbl] = community
            all_communities.append(community)

            for nid in members:
                node_assignments[nid].append(community.id)

        level_partitions.append(label_to_community)

    # Link parent communities across levels
    _link_parents(level_partitions)

    return CommunityResult(
        communities=all_communities,
        node_assignments=node_assignments,
        num_levels=len(resolutions),
        modularity_scores=modularity_scores,
    )


def _singleton_communities(node_ids: list[str], num_levels: int) -> CommunityResult:
    """Create singleton communities for a graph with no edges."""
    all_communities: list[Community] = []
    node_assignments: dict[str, list[str]] = {nid: [] for nid in node_ids}

    for level in range(num_levels):
        for i, nid in enumerate(node_ids):
            community = Community(
                id=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"c-{level}-{i}-singleton")),
                level=level,
                member_ids=[nid],
                size=1,
            )
            all_communities.append(community)
            node_assignments[nid].append(community.id)

    return CommunityResult(
        communities=all_communities,
        node_assignments=node_assignments,
        num_levels=num_levels,
        modularity_scores=[0.0] * num_levels,
    )


def _link_parents(level_partitions: list[dict[int, Community]]) -> None:
    """Link child communities to parent communities across levels.

    A child community's parent is the coarser-level community that contains
    the majority of its members.
    """
    for level_idx in range(len(level_partitions) - 1):
        child_communities = level_partitions[level_idx]
        parent_communities = level_partitions[level_idx + 1]

        # Build reverse map: node_id -> parent community
        node_to_parent: dict[str, Community] = {}
        for community in parent_communities.values():
            for nid in community.member_ids:
                node_to_parent[nid] = community

        # Assign parent by majority vote
        for community in child_communities.values():
            parent_votes: dict[str, int] = {}
            for nid in community.member_ids:
                parent = node_to_parent.get(nid)
                if parent:
                    parent_votes[parent.id] = parent_votes.get(parent.id, 0) + 1

            if parent_votes:
                community.parent_id = max(
                    parent_votes,
                    key=parent_votes.get,  # type: ignore[arg-type]
                )


async def summarize_communities(
    result: CommunityResult,
    nodes: list[dict[str, Any]],
    *,
    llm_service: str = "bedrock",
    model_id: str = "",
) -> CommunityResult:
    """Generate title and summary for each community using LLM.

    Groups member node names and generates a short description.
    Returns the same CommunityResult with title/summary fields populated.

    Args:
        result: Community detection result to summarize.
        nodes: Original node dicts (must have 'id' and 'name' keys).
        llm_service: LLM service to use (bedrock, openai, ollama).
        model_id: Model identifier (uses default per service if empty).

    Returns:
        The input CommunityResult with title/summary fields populated.
    """
    # Build id → name lookup
    id_to_name: dict[str, str] = {
        n["id"]: n.get("name", n.get("label", n["id"])) for n in nodes
    }

    for community in result.communities:
        member_names = [id_to_name.get(mid, mid) for mid in community.member_ids[:20]]
        # Placeholder: generate title from member names
        # In production, this calls an LLM for richer summaries
        community.title = _generate_title(member_names)
        community.summary = _generate_summary(member_names, community.level)

    return result


def _generate_title(member_names: list[str]) -> str:
    """Generate a placeholder title from member names."""
    if not member_names:
        return "Empty Community"
    if len(member_names) <= 3:
        return ", ".join(member_names)
    return f"{member_names[0]}, {member_names[1]} +{len(member_names) - 2} more"


def _generate_summary(member_names: list[str], level: int) -> str:
    """Generate a placeholder summary."""
    if not member_names:
        return ""
    return (
        f"Level-{level} community with {len(member_names)} members: "
        f"{', '.join(member_names[:5])}" + ("..." if len(member_names) > 5 else "")
    )


async def publish_communities(
    result: CommunityResult,
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    username: str = "neo4j",
    password: str = "",
) -> dict[str, int]:
    """Write community nodes and membership edges to Neo4j.

    Creates :Community nodes and :BELONGS_TO relationships linking
    entity nodes to their communities.

    Args:
        result: Community detection result.
        database_uri: Neo4j bolt URI.
        database_name: Neo4j database name.
        username: Neo4j username.
        password: Neo4j password.

    Returns:
        Dict with counts of created communities and membership edges.
    """
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(database_uri, auth=(username, password))
    communities_created = 0
    memberships_created = 0

    async with driver.session(database=database_name) as session:
        for community in result.communities:
            # Create community node
            cypher = (
                "MERGE (c:Community {id: $id}) "
                "SET c.level = $level, c.title = $title, "
                "c.summary = $summary, c.size = $size, "
                "c.parent_id = $parent_id"
            )
            await session.run(
                cypher,
                {
                    "id": community.id,
                    "level": community.level,
                    "title": community.title,
                    "summary": community.summary,
                    "size": community.size,
                    "parent_id": community.parent_id,
                },
            )
            communities_created += 1

            # Create membership edges
            for member_id in community.member_ids:
                membership_cypher = (
                    "MATCH (n {id: $member_id}) "
                    "MATCH (c:Community {id: $community_id}) "
                    "MERGE (n)-[:BELONGS_TO]->(c)"
                )
                await session.run(
                    membership_cypher,
                    {"member_id": member_id, "community_id": community.id},
                )
                memberships_created += 1

        # Create parent-child edges between communities
        for community in result.communities:
            if community.parent_id:
                parent_cypher = (
                    "MATCH (child:Community {id: $child_id}) "
                    "MATCH (parent:Community {id: $parent_id}) "
                    "MERGE (child)-[:PART_OF]->(parent)"
                )
                await session.run(
                    parent_cypher,
                    {
                        "child_id": community.id,
                        "parent_id": community.parent_id,
                    },
                )

    await driver.close()
    return {
        "communities_created": communities_created,
        "memberships_created": memberships_created,
    }


def communities_to_records(result: CommunityResult) -> list[dict[str, Any]]:
    """Convert communities to flat records for Parquet export.

    Each record contains all community fields in a flat dict format
    suitable for writing to columnar storage.

    Args:
        result: Community detection result.

    Returns:
        List of flat dicts, one per community.
    """
    records: list[dict[str, Any]] = []
    for community in result.communities:
        records.append(
            {
                "community_id": community.id,
                "level": community.level,
                "title": community.title,
                "summary": community.summary,
                "member_ids": community.member_ids,
                "member_count": community.size,
                "parent_id": community.parent_id,
            }
        )
    return records
