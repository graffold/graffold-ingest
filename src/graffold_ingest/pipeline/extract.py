"""LLM-powered schema-free entity extraction."""

from __future__ import annotations

from ..connectors.base import Document, ExtractionResult

EXTRACTION_PROMPT = """You are building a drug discovery knowledge graph. Extract ALL entities and relationships from this pharmaceutical research text.

Entity types to look for:
- Target (protein/gene drug targets — include organism, function, essentiality)
- Disease (conditions, pathologies)
- Compound (drugs, molecules, chemical classes — include mechanism)
- Mechanism (biological mechanisms, pathways)
- Organism (species, pathogens)
- Evidence (publications — include PMID, DOI, journal, year)
- Decision (kill/advance/hold decisions with rationale)

Relationship types:
- INHIBITS, ACTIVATES (compound->target or target->pathway)
- TARGETS_DISEASE (target->disease)
- VALIDATED_BY (target->evidence)
- KILLED_BECAUSE (target->decision with reason)
- PROPOSED_FOR (compound->disease)
- PART_OF (mechanism->pathway, target->mechanism)
- SELECTIVE_OVER (target has selectivity over host orthologue)

Return ONLY valid JSON:
{{"nodes": [{{"id": "unique_id", "label": "EntityType", "name": "display name", "properties": {{"key": "value"}}}}], "edges": [{{"source": "node_id", "target": "node_id", "type": "RELATIONSHIP_TYPE"}}]}}

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
            import re

            # Strip markdown fences if present
            cleaned = re.sub(r"```(?:json)?\s*\n?", "", raw).strip()
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()

            data = json.loads(cleaned)
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


async def extract_entities_parallel(
    documents: list[Document],
    llm_service: str = "anthropic",
    model_id: str = "",
    max_concurrent: int = 3,
) -> list[ExtractionResult]:
    """Parallel LLM extraction — processes multiple chunks concurrently."""
    import asyncio
    import json
    import re

    semaphore = asyncio.Semaphore(max_concurrent)

    async def _extract_one(doc: Document) -> ExtractionResult:
        async with semaphore:
            text = doc.content[:8000]
            prompt = EXTRACTION_PROMPT.format(text=text)
            try:
                raw = await _call_llm(prompt, llm_service, model_id)
                cleaned = re.sub(r"```(?:json)?\s*\n?", "", raw).strip()
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3].strip()
                data = json.loads(cleaned)
                return ExtractionResult(
                    nodes=data.get("nodes", []),
                    edges=data.get("edges", []),
                    source_doc_id=doc.id,
                )
            except Exception:
                return ExtractionResult(source_doc_id=doc.id)

    results = await asyncio.gather(*[_extract_one(doc) for doc in documents])
    return list(results)


async def _call_llm(prompt: str, service: str, model_id: str) -> str:
    """Call an LLM service. Supports anthropic, bedrock, openai, openrouter, claude-code, ollama."""
    if service == "anthropic":
        return await _call_anthropic(prompt, model_id or "claude-opus-5")
    elif service == "bedrock":
        return await _call_bedrock(prompt, model_id or "anthropic.claude-3-haiku-20240307-v1:0")
    elif service == "bedrock-llama":
        return await _call_bedrock_llama(prompt, model_id or "us.meta.llama3-3-70b-instruct-v1:0")
    elif service == "openai":
        return await _call_openai(prompt, model_id or "gpt-4o-mini")
    elif service == "openrouter":
        return await _call_openrouter(prompt, model_id or "anthropic/claude-sonnet-4")
    elif service == "claude-code":
        return await _call_claude_code(prompt, model_id or "claude-opus-5")
    elif service == "ollama":
        return await _call_ollama(prompt, model_id or "qwen3:1.7b")
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


async def _call_bedrock_llama(prompt: str, model_id: str) -> str:
    """Call AWS Bedrock Llama models (different prompt format than Claude).

    Uses Llama's instruct chat template + max_gen_len. Runs the blocking
    boto3 call in a thread so the pipeline stays async.
    """
    import asyncio
    import json
    import os

    import boto3

    region = os.getenv("AWS_REGION", "us-east-1")
    client = boto3.client("bedrock-runtime", region_name=region)
    formatted = (
        "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\n"
        f"{prompt}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n"
    )
    body = json.dumps({
        "prompt": formatted,
        "max_gen_len": 4096,
        "temperature": 0.1,
    })

    def _invoke() -> str:
        resp = client.invoke_model(modelId=model_id, body=body)
        return json.loads(resp["body"].read()).get("generation", "")

    return await asyncio.to_thread(_invoke)


async def _call_anthropic(prompt: str, model_id: str) -> str:
    """Call Anthropic API directly (fastest path with API key)."""
    import os

    import httpx

    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model_id,
                "max_tokens": 16384,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        resp.raise_for_status()
        # Extract text from content blocks (skip thinking blocks)
        for block in resp.json()["content"]:
            if block.get("type") == "text":
                return block["text"]
        return ""


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
    import os

    import httpx

    base = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            f"{base}/api/generate",
            json={"model": model_id, "prompt": prompt, "stream": False},
        )
        return resp.json().get("response", "")


async def _call_openrouter(prompt: str, model_id: str) -> str:
    """Call OpenRouter (access to Claude, GPT, open models via one key)."""
    import os

    import httpx

    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY not set")

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model_id,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 4096,
            },
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]


async def _call_claude_code(prompt: str, model_id: str) -> str:
    """Call Claude via the claude CLI (uses Team/org auth)."""
    import asyncio as _asyncio

    proc = await _asyncio.create_subprocess_exec(
        "claude", "--model", model_id, "--print", prompt,
        stdout=_asyncio.subprocess.PIPE,
        stderr=_asyncio.subprocess.PIPE,
        env=_strip_anthropic_env(),
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"claude CLI failed: {stderr.decode()[:500]}")
    return stdout.decode()


def _strip_anthropic_env() -> dict[str, str]:
    """Return os.environ minus ANTHROPIC_* vars that interfere with Team auth."""
    import os
    return {
        k: v for k, v in os.environ.items()
        if not k.startswith("ANTHROPIC_")
    }
