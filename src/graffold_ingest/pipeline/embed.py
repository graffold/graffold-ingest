"""Embedding generation for graph nodes."""

from __future__ import annotations


async def generate_embeddings(
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    service: str = "bedrock",
) -> int:
    """Generate vector embeddings for all nodes missing them.

    Returns count of nodes embedded.
    """
    # Stub — will be implemented with the embedding factory
    # from graffold-api or a local sentence-transformers model
    raise NotImplementedError("Embedding generation not yet implemented")
