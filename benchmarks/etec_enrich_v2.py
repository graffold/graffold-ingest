"""ETEC-pigs literature enrichment v2 — FULL-TEXT via Europe PMC + Llama 3.3 70B.

Improvements over v1:
- Europe PMC full-text (OA) as primary source (~20K chars vs ~1800 abstract)
- PubMed abstracts as fallback for non-OA papers
- Chunks large full-text papers (4000 chars) so extraction fits token budget
- Fuzzy entity resolution (collapses "Heat-stable toxin" variants)
- ETEC-relevance filter (drops off-topic content)
- JSON salvage for truncated Llama output
- Checkpointed for background/resume

Writes to ~/.graffold/parquet/etec-pigs (append-only, on top of seed).

Usage:
    AWS_REGION=us-east-1 python benchmarks/etec_enrich_v2.py
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path

from graffold_ingest.connectors.base import Document, ExtractionResult
from graffold_ingest.connectors.europepmc import EuropePMCConnector
from graffold_ingest.connectors.pubmed import PubMedConnector
from graffold_ingest.pipeline.chunk import chunk_documents
from graffold_ingest.pipeline.extract import EXTRACTION_PROMPT, _call_bedrock_llama
from graffold_ingest.pipeline.publish_parquet import publish_to_parquet
from graffold_ingest.resolvers.local import EntityResolver

OUTPUT_DIR = Path.home() / ".graffold" / "parquet" / "etec-pigs"
CHECKPOINT = OUTPUT_DIR / ".enrich_v2_checkpoint.json"
MODEL = "us.meta.llama3-3-70b-instruct-v1:0"
MAX_CONCURRENT = 5
CHUNK_SIZE = 4000

QUERIES = [
    "F18 ETEC adhesion piglet FedF",
    "F4 ETEC FaeG fimbriae adhesion swine",
    "post-weaning diarrhea enterotoxigenic Escherichia coli piglet",
    "edema disease pig Shiga toxin STx2e",
    "ETEC heat-labile enterotoxin LT neutralization",
    "ETEC heat-stable enterotoxin STa STb piglet",
    "LTB GM1 ganglioside binding intestinal epithelium",
    "enterotoxin binding feed additive livestock",
    "FedF F18 receptor porcine intestinal glycosphingolipid",
    "aminopeptidase N F4 ETEC receptor piglet",
    "tight junction ETEC intestinal permeability pig",
    "probiotic Bacillus F18 ETEC piglet weaning",
    "yeast mannan oligosaccharide ETEC adhesion swine",
    "medium chain fatty acids ETEC antimicrobial pig",
    "competitive exclusion ETEC probiotic piglet",
    "anti-virulence strategy enterotoxigenic E coli",
    "IPEC-J2 cells ETEC adhesion assay",
    "IPEC-1 porcine intestinal epithelial cell ETEC",
    "TEER transepithelial resistance ETEC piglet",
    "zinc oxide alternative post-weaning diarrhea piglet",
    "nanobody VHH F18 fimbriae inhibition E coli",
    "FaeG LTB vaccine ETEC mucosal immunity piglet",
    "STb enterotoxin binding porcine intestine",
    "egg yolk IgY antibody ETEC piglet passive immunity",
    "galactomannan ETEC adhesion inhibition pig",
]

# Relevance gate: paper must mention ETEC/pig context, not pure crypto/other
RELEVANT = re.compile(
    r"\b(ETEC|F18|F4|K88|F5|F6|F41|enterotoxigenic|piglet|weaning|"
    r"post-weaning|FedF|FaeG|porcine|swine|IPEC|enterotoxin|heat-labile|"
    r"heat-stable|edema disease|E\.?\s?coli)\b",
    re.IGNORECASE,
)


def _load_checkpoint() -> set[str]:
    if CHECKPOINT.exists():
        try:
            return set(json.loads(CHECKPOINT.read_text()).get("processed", []))
        except Exception:
            return set()
    return set()


def _save_checkpoint(processed: set[str]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT.write_text(json.dumps({"processed": sorted(processed), "at": time.time()}))


def _salvage_json(text: str) -> dict:
    """Recover complete node/edge objects from truncated JSON."""
    nodes, edges = [], []
    for key, bucket in (("nodes", nodes), ("edges", edges)):
        m = re.search(rf'"{key}"\s*:\s*\[', text)
        if not m:
            continue
        i, depth, obj_start = m.end(), 0, -1
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


async def _extract_one(chunk, sem):
    async with sem:
        prompt = EXTRACTION_PROMPT.format(text=chunk.content[:8000])
        try:
            raw = await _call_bedrock_llama(prompt, MODEL)
            cleaned = re.sub(r"```(?:json)?\s*\n?", "", raw).strip()
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()
            s, e = cleaned.find("{"), cleaned.rfind("}")
            if s >= 0 and e > s:
                cleaned = cleaned[s:e + 1]
            try:
                data = json.loads(cleaned)
            except json.JSONDecodeError:
                data = _salvage_json(cleaned)
            return ExtractionResult(
                nodes=data.get("nodes", []),
                edges=data.get("edges", []),
                source_doc_id=chunk.id,
            )
        except Exception:
            return None


async def main():
    pm = PubMedConnector()
    epmc = EuropePMCConnector()
    resolver = EntityResolver(enable_fuzzy=True)  # fuzzy on — collapse variants
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    processed = _load_checkpoint()
    print(f"ETEC full-text enrichment (Llama 3.3 70B, fuzzy dedup)")
    print(f"Already processed: {len(processed)} papers\n")

    # ─── Gather: Europe PMC full-text primary, PubMed fallback ─────────────
    all_docs: dict[str, Document] = {}
    for q in QUERIES:
        epmc_docs = await epmc.fetch(query=q, limit=12, full_text=True)
        pm_docs = await pm.fetch(query=q, limit=10)
        for d in epmc_docs + pm_docs:
            key = d.metadata.get("pmid") or d.id
            if not key or key in processed or key in all_docs:
                continue
            # Relevance gate
            if not RELEVANT.search(d.content[:3000]):
                continue
            all_docs[key] = d
        print(f"  {q[:48]:48s} (unique relevant: {len(all_docs)})")

    docs = list(all_docs.values())
    fulltext_count = sum(1 for d in docs if len(d.content) > 5000)
    print(f"\n{len(docs)} papers ({fulltext_count} full-text, {len(docs)-fulltext_count} abstract)")

    # ─── Chunk large papers ────────────────────────────────────────────────
    chunks = chunk_documents(docs, chunk_size=CHUNK_SIZE)
    print(f"Chunked into {len(chunks)} pieces\n")

    # ─── Extract in batches, publish + checkpoint each ─────────────────────
    t0 = time.time()
    total_ent = total_rel = 0
    BATCH = 25
    for i in range(0, len(chunks), BATCH):
        batch = chunks[i:i + BATCH]
        results = await asyncio.gather(*[_extract_one(c, sem) for c in batch])
        results = [r for r in results if r and r.nodes]
        if results:
            all_n = [n for r in results for n in r.nodes]
            all_e = [e for r in results for e in r.edges]
            merged_n, merged_e = resolver.resolve(all_n, all_e)
            combined = [ExtractionResult(nodes=merged_n, edges=merged_e,
                                          source_doc_id=f"etec:ft:batch-{i // BATCH}")]
            counts = await publish_to_parquet(combined, output_dir=OUTPUT_DIR,
                                               run_id=f"ft-batch-{i // BATCH}")
            total_ent += counts["entities_written"]
            total_rel += counts["relationships_written"]

        # checkpoint by source doc
        for c in batch:
            base = c.id.split("_chunk")[0].replace("pmid:", "")
            processed.add(base)
        _save_checkpoint(processed)
        print(f"  batch {i // BATCH + 1}/{(len(chunks) + BATCH - 1) // BATCH}: "
              f"+{sum(len(r.nodes) for r in results)} ent "
              f"[{time.time() - t0:.0f}s]")

    print(f"\n{'=' * 60}")
    print(f"  FULL-TEXT ENRICHMENT COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Papers:            {len(docs)} ({fulltext_count} full-text)")
    print(f"  Chunks extracted:  {len(chunks)}")
    print(f"  New entities:      {total_ent}")
    print(f"  New relationships: {total_rel}")
    print(f"  Runtime:           {(time.time() - t0) / 60:.1f} min")


if __name__ == "__main__":
    asyncio.run(main())
