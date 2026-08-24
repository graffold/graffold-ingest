"""Local embeddings via Ollama — generates vectors for entity search.

Uses nomic-embed-text (768-dim) via Ollama's /api/embeddings endpoint.
No cloud dependency, runs on the same machine as the ingest pipeline.

Falls back gracefully if Ollama is not running.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")
BATCH_SIZE = 32


def embed_texts(texts: list[str], model: str = EMBED_MODEL) -> list[list[float]]:
    """Generate embeddings for a list of texts via Ollama.

    Returns list of 768-dim vectors. Returns empty list on failure.
    """
    if not texts:
        return []

    embeddings: list[list[float]] = []
    url = f"{OLLAMA_URL}/api/embeddings"

    try:
        with httpx.Client(timeout=60.0) as client:
            for i in range(0, len(texts), BATCH_SIZE):
                batch = texts[i:i + BATCH_SIZE]
                for text in batch:
                    resp = client.post(url, json={"model": model, "prompt": text})
                    if resp.status_code == 200:
                        embeddings.append(resp.json()["embedding"])
                    else:
                        embeddings.append([])
    except Exception as e:
        logger.warning("Ollama embeddings failed: %s", e)
        return []

    return embeddings


def embed_single(text: str, model: str = EMBED_MODEL) -> list[float]:
    """Embed a single text. Returns empty list on failure."""
    result = embed_texts([text], model=model)
    return result[0] if result else []


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two vectors."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)
