"""Ingest a real Atlas ETEC run into the etec-pigs knowledge graph.

Layers Atlas's drug-discovery reasoning (disease map, candidates, kills,
board decisions) ON TOP of the literature + institutional seed.

This closes the loop: literature (external knowledge) + Atlas (decisions)
in one graph. Future Atlas runs query this for prior knowledge.

Usage:
    AWS_REGION=us-east-1 python benchmarks/etec_ingest_atlas.py
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path

from graffold_ingest.connectors.base import Document, ExtractionResult
from graffold_ingest.pipeline.chunk import chunk_documents
from graffold_ingest.pipeline.extract import EXTRACTION_PROMPT, _call_bedrock_llama
from graffold_ingest.pipeline.publish_parquet import publish_to_parquet
from graffold_ingest.pipeline.section_parser import remove_sections
from graffold_ingest.resolvers.local import EntityResolver

OUTPUT_DIR = Path.home() / ".graffold" / "parquet" / "etec-pigs"
MODEL = "us.meta.llama3-3-70b-instruct-v1:0"
MAX_CONCURRENT = 4

ATLAS_RUN = Path(
    "/Users/apple/Developer/agteria/atlas/programs/_analysis/bakeoff/"
    "wave1-open/work/gpt-oss-120b/final_arbiter/v1"
)

# The high-signal phase files (skip raw/argus duplicates)
PHASE_FILES = [
    "phase-1-disease-map.md",
    "phase-1a-anomaly-map.md",
    "phase-1b-bottleneck-consensus.md",
    "phase-2-failure-analysis.md",
    "phase-2b-competitive-landscape.md",
    "phase-3-candidates.md",
    "phase-3b-survey-report.md",
    "phase-3c-literature-sweep.md",
    "phase-3d-feasibility-report.md",
    "phase-4-kill-report.md",
    "phase-4b-board-decision.md",
    "phase-5-decision-memo.md",
    "phase-5-coverage-map.md",
    "phase-5-evidence-register.md",
]


async def _extract_one(doc, sem):
    async with sem:
        text = remove_sections(doc.content)[:8000]
        prompt = EXTRACTION_PROMPT.format(text=text)
        try:
            raw = await _call_bedrock_llama(prompt, MODEL)
            cleaned = re.sub(r"```(?:json)?\s*\n?", "", raw).strip()
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()
            start, end = cleaned.find("{"), cleaned.rfind("}")
            if start >= 0 and end > start:
                cleaned = cleaned[start:end + 1]
            try:
                data = json.loads(cleaned)
            except json.JSONDecodeError:
                data = _salvage_json(cleaned)
            return ExtractionResult(
                nodes=data.get("nodes", []),
                edges=data.get("edges", []),
                source_doc_id=doc.id,
            )
        except Exception as e:
            print(f"    extract fail [{doc.id}]: {str(e)[:60]}")
            return None


def _salvage_json(text: str) -> dict:
    """Recover nodes/edges from truncated JSON by extracting complete objects."""
    nodes, edges = [], []
    # Grab the nodes array region and pull complete {...} objects
    for key, bucket in (("nodes", nodes), ("edges", edges)):
        m = re.search(rf'"{key}"\s*:\s*\[', text)
        if not m:
            continue
        i = m.end()
        depth = 0
        obj_start = -1
        while i < len(text):
            c = text[i]
            if c == "{":
                if depth == 0:
                    obj_start = i
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0 and obj_start >= 0:
                    try:
                        bucket.append(json.loads(text[obj_start:i + 1]))
                    except json.JSONDecodeError:
                        pass
                    obj_start = -1
            elif c == "]" and depth == 0:
                break
            i += 1
    return {"nodes": nodes, "edges": edges}


async def main():
    resolver = EntityResolver(enable_fuzzy=False)
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    print(f"Ingesting Atlas ETEC run: {ATLAS_RUN.name}")
    print(f"  Source: gpt-oss-120b bakeoff, phibro-etec\n")

    # Load phase files as Documents
    docs = []
    for fname in PHASE_FILES:
        fpath = ATLAS_RUN / fname
        if not fpath.exists():
            continue
        content = fpath.read_text()
        if len(content) < 200:  # skip stubs
            continue
        docs.append(Document(
            id=f"atlas:phibro-etec:{fname.replace('.md', '')}",
            content=content,
            source_type="atlas",
            title=fname,
            metadata={"program": "phibro-etec-piglet-v2", "phase": fname},
        ))
    print(f"Loaded {len(docs)} phase files")

    # Chunk large phase files (some candidates docs are huge)
    chunks = chunk_documents(docs, chunk_size=3500)
    print(f"Chunked into {len(chunks)} pieces\n")

    t0 = time.time()
    tasks = [_extract_one(c, sem) for c in chunks]
    results = await asyncio.gather(*tasks)
    results = [r for r in results if r and r.nodes]

    all_nodes = [n for r in results for n in r.nodes]
    all_edges = [e for r in results for e in r.edges]
    print(f"Extracted: {len(all_nodes)} entities, {len(all_edges)} relationships")

    # Resolve — this is where Atlas entities MERGE with literature entities
    # (e.g. Atlas "CpTrxR" / "FedF" / "F18" collapse into the existing nodes)
    merged_n, merged_e = resolver.resolve(all_nodes, all_edges)
    print(f"Resolved: {len(merged_n)} entities ({len(all_nodes) - len(merged_n)} merged into existing)")

    combined = [ExtractionResult(nodes=merged_n, edges=merged_e,
                                  source_doc_id="atlas:phibro-etec-piglet-v2")]
    counts = await publish_to_parquet(combined, output_dir=OUTPUT_DIR,
                                       run_id="atlas-phibro-etec")

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"  ATLAS INGESTION COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Phase files:   {len(docs)}")
    print(f"  New entities:  {counts['entities_written']}")
    print(f"  New relationships: {counts['relationships_written']}")
    print(f"  Runtime:       {elapsed:.0f}s")
    print(f"  → {OUTPUT_DIR}")
    print(f"\n  The graph now has: literature (external) + Atlas (decisions)")
    print(f"  + institutional seed (kills, constraints, hypotheses)")


if __name__ == "__main__":
    asyncio.run(main())
