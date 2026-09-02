"""ETEC-pigs literature enrichment — fetch papers, extract via Llama 3.3 70B, append to KG.

Runs on top of the seed backbone (run etec_seed.py first).
Writes to ~/.graffold/parquet/etec-pigs (append-only).

Designed to run in the background. Checkpoints after each query so a
crash/restart resumes cleanly (skips already-processed PMIDs).

Usage:
    AWS_REGION=us-east-1 python benchmarks/etec_enrich.py
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from graffold_ingest.connectors.base import ExtractionResult
from graffold_ingest.connectors.europepmc import EuropePMCConnector
from graffold_ingest.connectors.pubmed import PubMedConnector
from graffold_ingest.pipeline.extract import EXTRACTION_PROMPT, _call_bedrock_llama
from graffold_ingest.pipeline.publish_parquet import publish_to_parquet
from graffold_ingest.pipeline.section_parser import remove_sections
from graffold_ingest.resolvers.local import EntityResolver

OUTPUT_DIR = Path.home() / ".graffold" / "parquet" / "etec-pigs"
CHECKPOINT = OUTPUT_DIR / ".enrich_checkpoint.json"
MODEL = "us.meta.llama3-3-70b-instruct-v1:0"
MAX_CONCURRENT = 4

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
]

# Cited in the intake form — pull directly
CITED_PMIDS = ["20399188", "28720278", "38000000"]  # Liang 2010, Yu 2017 (+ Yu 2024 via query)


def _load_checkpoint() -> set[str]:
    if CHECKPOINT.exists():
        try:
            return set(json.loads(CHECKPOINT.read_text()).get("processed_pmids", []))
        except Exception:
            return set()
    return set()


def _save_checkpoint(processed: set[str]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT.write_text(json.dumps({"processed_pmids": sorted(processed),
                                      "updated_at": time.time()}))


async def _extract_one(doc, resolver, sem) -> ExtractionResult | None:
    async with sem:
        # Strip References/Acknowledgements, cap length
        text = remove_sections(doc.content)[:8000]
        prompt = EXTRACTION_PROMPT.format(text=text)
        try:
            raw = await _call_bedrock_llama(prompt, MODEL)
            import re
            cleaned = re.sub(r"```(?:json)?\s*\n?", "", raw).strip()
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()
            # Llama sometimes adds prose before/after JSON — grab the object
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start >= 0 and end > start:
                cleaned = cleaned[start:end + 1]
            data = json.loads(cleaned)
            return ExtractionResult(
                nodes=data.get("nodes", []),
                edges=data.get("edges", []),
                source_doc_id=doc.id,
            )
        except Exception as e:
            print(f"    extract fail [{doc.id}]: {str(e)[:60]}")
            return None


async def main():
    pm = PubMedConnector()
    epmc = EuropePMCConnector()
    resolver = EntityResolver(enable_fuzzy=False)
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    processed = _load_checkpoint()
    print(f"ETEC enrichment starting (model: Llama 3.3 70B, {MAX_CONCURRENT} concurrent)")
    print(f"Already processed: {len(processed)} papers\n")

    t_start = time.time()
    total_new_entities = 0
    total_new_rels = 0
    total_papers = 0

    # Gather all unique docs first
    all_docs: dict[str, object] = {}
    for q in QUERIES:
        pm_docs = await pm.fetch(query=q, limit=15)
        epmc_docs = await epmc.fetch(query=q, limit=10, full_text=True)
        for d in pm_docs + epmc_docs:
            pmid = d.metadata.get("pmid") or d.id
            if pmid and pmid not in all_docs and pmid not in processed:
                all_docs[pmid] = d
        print(f"  fetched: {q[:45]:45s} (total unique new: {len(all_docs)})")

    # Cited papers
    cited = await pm.fetch(pmids=CITED_PMIDS)
    for d in cited:
        pmid = d.metadata.get("pmid") or d.id
        if pmid not in all_docs and pmid not in processed:
            all_docs[pmid] = d

    docs = list(all_docs.values())
    print(f"\nExtracting from {len(docs)} papers...\n")

    # Process in batches, publish + checkpoint each batch
    BATCH = 20
    for i in range(0, len(docs), BATCH):
        batch = docs[i:i + BATCH]
        tasks = [_extract_one(d, resolver, sem) for d in batch]
        results = await asyncio.gather(*tasks)
        results = [r for r in results if r and r.nodes]

        if results:
            all_nodes = [n for r in results for n in r.nodes]
            all_edges = [e for r in results for e in r.edges]
            merged_n, merged_e = resolver.resolve(all_nodes, all_edges)
            combined = [ExtractionResult(nodes=merged_n, edges=merged_e,
                                          source_doc_id=f"etec:lit:batch-{i // BATCH}")]
            counts = await publish_to_parquet(combined, output_dir=OUTPUT_DIR,
                                               run_id=f"lit-batch-{i // BATCH}")
            total_new_entities += counts["entities_written"]
            total_new_rels += counts["relationships_written"]

        for d in batch:
            processed.add(d.metadata.get("pmid") or d.id)
        total_papers += len(batch)
        _save_checkpoint(processed)

        elapsed = time.time() - t_start
        print(f"  batch {i // BATCH + 1}: +{sum(len(r.nodes) for r in results)} ent, "
              f"+{sum(len(r.edges) for r in results)} rel "
              f"[{total_papers}/{len(docs)} papers, {elapsed:.0f}s]")

    elapsed = time.time() - t_start
    print(f"\n{'=' * 60}")
    print(f"  ENRICHMENT COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Papers processed: {total_papers}")
    print(f"  New entities:     {total_new_entities}")
    print(f"  New relationships:{total_new_rels}")
    print(f"  Runtime:          {elapsed / 60:.1f} min")
    print(f"  Output:           {OUTPUT_DIR}")


if __name__ == "__main__":
    asyncio.run(main())
