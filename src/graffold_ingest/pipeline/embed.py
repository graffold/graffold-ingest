"""Embedding generation and Cloudflare Vectorize upload.

Generates embeddings via CF Workers AI, then upserts to CF Vectorize.
Self-contained — no dependency on graffold-api.

Requires: CF_ACCOUNT_ID, CF_API_TOKEN env vars.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

_CF_EMBED_MODEL = "@cf/baai/bge-base-en-v1.5"
_VECTORIZE_INDEX = "graffold-embeddings"
_BATCH_SIZE = 20


def _cf_account() -> tuple[str, str]:
    account_id = os.getenv("CF_ACCOUNT_ID", "")
    api_token = os.getenv("CF_API_TOKEN", "")
    return account_id, api_token


def generate_embeddings(texts: list[str]) -> list[list[float]]:
    """Generate embeddings via Cloudflare Workers AI.

    Uses bge-base-en-v1.5 (768-dim) hosted on CF Workers AI.
    """
    account_id, api_token = _cf_account()
    if not account_id or not api_token:
        raise ValueError("Set CF_ACCOUNT_ID and CF_API_TOKEN")

    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{_CF_EMBED_MODEL}"
    all_embeddings: list[list[float]] = []

    for i in range(0, len(texts), _BATCH_SIZE):
        batch = texts[i : i + _BATCH_SIZE]
        body = json.dumps({"text": batch}).encode()
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read())

        if result.get("success"):
            data = result.get("result", {}).get("data", [])
            all_embeddings.extend(data)
        else:
            errors = result.get("errors", [])
            raise RuntimeError(f"CF embedding failed: {errors}")

    return all_embeddings


def upsert_to_vectorize(
    vectors: list[dict[str, Any]],
    index_name: str = _VECTORIZE_INDEX,
) -> dict[str, Any]:
    """Upsert vectors to Cloudflare Vectorize.

    Each vector dict: {"id": str, "values": list[float], "metadata": dict}
    """
    account_id, api_token = _cf_account()
    if not account_id or not api_token or not vectors:
        return {"success": False, "error": "Missing credentials or empty vectors"}

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
        f"/vectorize/v2/indexes/{index_name}/insert"
    )

    # Vectorize uses ndjson format
    ndjson = "\n".join(json.dumps(v) for v in vectors)
    req = urllib.request.Request(
        url,
        data=ndjson.encode(),
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/x-ndjson",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.error("Vectorize upsert failed: %s", e)
        return {"success": False, "error": str(e)}


async def embed_and_upload(
    nodes: list[dict[str, Any]],
    database_uri: str = "bolt://localhost:7687",
    database_name: str = "neo4j",
    index_name: str = _VECTORIZE_INDEX,
) -> int:
    """Generate embeddings for extracted nodes and upload to Vectorize.

    Embeds node names + types as text, uploads vectors with node metadata.
    Returns count of vectors upserted.
    """
    if not nodes:
        return 0

    # Build text representations for embedding
    texts = []
    node_ids = []
    for node in nodes:
        name = node.get("name", "")
        node_type = node.get("type", node.get("label", "Entity"))
        text = f"{node_type}: {name}" if name else ""
        if text:
            texts.append(text)
            node_ids.append(node["id"])

    if not texts:
        return 0

    # Generate embeddings
    embeddings = generate_embeddings(texts)

    # Build vectors for Vectorize
    vectors = []
    for node_id, embedding, text in zip(node_ids, embeddings, texts, strict=True):
        vectors.append({
            "id": node_id,
            "values": embedding,
            "metadata": {"text": text, "node_id": node_id},
        })

    # Upsert in batches
    total = 0
    for i in range(0, len(vectors), 100):
        batch = vectors[i : i + 100]
        result = upsert_to_vectorize(batch, index_name=index_name)
        if result.get("success") is not False:
            total += len(batch)

    logger.info("Embedded and uploaded %d/%d vectors to Vectorize", total, len(vectors))
    return total
