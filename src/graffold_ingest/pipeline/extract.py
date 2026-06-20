"""LLM-powered schema-free entity extraction."""

from __future__ import annotations

from typing import Any

from ..connectors.base import Document, ExtractionResult


EXTRACTION_PROMPT = """Extract all entities and relationships from this text.
Return JSON with:
- "nodes": list of {{"id": "unique_id", "label": "EntityType", "name": "display name", "properties": {{}}}}
- "edges": list of {{"source": "node_id", "target": "node_id", "type": "RELATIONSHIP_TYPE", "properties": {{}}}}

Discover entity types and relationship types from the content. Do NOT use a predefined schema.

Text:
{text}
"""


async def extract_entities(
    documents: list[Document],
    llm_service: str = "bedrock",
    model_id: str = "",
) -> list[ExtractionResult]:
    """LLM-powered schema-free entity extraction.

    Discovers entities and relationships without a predefined schema.
    Returns ExtractionResult per document.
    """
    results: list[ExtractionResult] = []

    for doc in documents:
        # Truncate content for LLM context window
        text = doc.content[:8000]
        prompt = EXTRACTION_PROMPT.format(text=text)

        try:
            raw = await _call_llm(prompt, llm_service, model_id)
            import json

            data = json.loads(raw)
            results.append(
                ExtractionResult(
                    nodes=data.get("nodes", []),
                    edges=data.get("edges", []),
                    source_doc_id=doc.id,
                )
            )
        except Exception:
            results.append(ExtractionResult(source_doc_id=doc.id))

    return results


async def _call_llm(prompt: str, service: str, model_id: str) -> str:
    """Call an LLM service. Supports bedrock, openai, ollama."""
    if service == "bedrock":
        return await _call_bedrock(prompt, model_id or "anthropic.claude-3-haiku-20240307-v1:0")
    elif service == "openai":
        return await _call_openai(prompt, model_id or "gpt-4o-mini")
    elif service == "ollama":
        return await _call_ollama(prompt, model_id or "llama3.1")
    else:
        raise ValueError(f"Unknown LLM service: {service}")


async def _call_bedrock(prompt: str, model_id: str) -> str:
    """Call AWS Bedrock."""
    import json

    import boto3

    client = boto3.client("bedrock-runtime")
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 4096,
    })
    response = client.invoke_model(modelId=model_id, body=body)
    result = json.loads(response["body"].read())
    return result["content"][0]["text"]


async def _call_openai(prompt: str, model_id: str) -> str:
    """Call OpenAI."""
    import openai

    client = openai.AsyncOpenAI()
    resp = await client.chat.completions.create(
        model=model_id,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.choices[0].message.content or ""


async def _call_ollama(prompt: str, model_id: str) -> str:
    """Call Ollama."""
    import httpx

    import os

    base = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            f"{base}/api/generate",
            json={"model": model_id, "prompt": prompt, "stream": False},
        )
        return resp.json().get("response", "")
