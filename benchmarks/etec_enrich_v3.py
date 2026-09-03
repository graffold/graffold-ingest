"""ETEC-pigs literature enrichment v3 — 1000-paper POC scale.

Broadened query set (49 queries) across ETEC core + adjacent biology
(adhesins, toxins, feed additives, host models, comparative pathogens)
to reach ~1000 unique papers — an order of magnitude over the 236-paper run.

Full-text via Europe PMC, abstract fallback, fuzzy dedup, relevance gate,
JSON salvage, checkpointed. Writes to etec-pigs-1k.

Usage:
    AWS_REGION=us-east-1 python benchmarks/etec_enrich_v3.py
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

OUTPUT_DIR = Path.home() / ".graffold" / "parquet" / "etec-pigs-1k"
CHECKPOINT = OUTPUT_DIR / ".enrich_v3_checkpoint.json"
MODEL = "us.meta.llama3-3-70b-instruct-v1:0"
MAX_CONCURRENT = 6
CHUNK_SIZE = 4000
PAPER_CAP = 1000
FULLTEXT_CAP = 400  # cap full-text fetches to control chunk explosion

QUERIES = [
    # Core ETEC
    "F18 ETEC adhesion piglet", "F4 ETEC FaeG fimbriae swine",
    "post-weaning diarrhea enterotoxigenic Escherichia coli",
    "edema disease pig Shiga toxin", "ETEC heat-labile enterotoxin",
    "ETEC heat-stable enterotoxin STa STb", "LTB GM1 ganglioside",
    # Adhesins & receptors
    "fimbrial adhesin Escherichia coli intestinal", "K88 K99 987P fimbriae pig",
    "bacterial adhesin receptor enterocyte", "F5 F41 fimbriae calf",
    "mannose-resistant hemagglutination E coli", "type 1 fimbriae FimH",
    # Toxins
    "cholera toxin B subunit mucosal", "guanylate cyclase C enterotoxin",
    "STa toxin receptor signaling", "enterotoxin structure function bacterial",
    "AB5 toxin cellular entry", "toxin neutralization antibody gut",
    # Feed additives / interventions
    "probiotic weaning piglet gut health", "Lactobacillus swine intestinal",
    "Bacillus subtilis animal feed", "yeast beta-glucan mannan swine",
    "medium chain fatty acids antibacterial", "organic acid feed additive pig",
    "essential oil antimicrobial livestock", "phytobiotic swine performance",
    "postbiotic gut barrier pig", "prebiotic oligosaccharide weaning",
    "competitive exclusion Salmonella poultry", "bacteriophage therapy E coli livestock",
    # Zinc / state of art
    "zinc oxide weaning diarrhea", "copper feed additive antimicrobial pig",
    # Host biology / models
    "IPEC-J2 intestinal epithelial barrier", "porcine intestinal organoid",
    "tight junction occludin claudin gut", "TEER transepithelial resistance model",
    "gut microbiome weaning transition piglet", "intestinal inflammation cytokine pig",
    "mucin secretion goblet cell intestinal", "antimicrobial peptide defensin gut",
    # Antibody / biologic
    "egg yolk IgY passive immunity livestock", "nanobody VHH bacterial neutralization",
    "monoclonal antibody enteric pathogen", "recombinant fimbrial vaccine pig",
    # Comparative / mechanism
    "enteropathogenic E coli attaching effacing", "Shiga toxin producing E coli cattle",
    "colibacillosis poultry virulence", "antimicrobial resistance E coli livestock",
]

# Relevance gate: paper must mention ETEC/pig context, not pure crypto/other
RELEVANT = re.compile(
    r"\b(ETEC|EPEC|STEC|F18|F4|F5|F6|F41|K88|K99|987P|enterotoxigenic|"
    r"piglet|weaning|post-weaning|nursery pig|FedF|FaeG|FimH|fimbria|adhesin|"
    r"porcine|swine|pig|calf|bovine|poultry|broiler|IPEC|intestinal epithel|"
    r"enterotoxin|heat-labile|heat-stable|Shiga|edema disease|colibacillosis|"
    r"probiotic|Lactobacillus|Bacillus|prebiotic|postbiotic|feed additive|"
    r"zinc oxide|tight junction|gut barrier|E\.?\s?coli|Escherichia)\b",
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

    # ─── Gather: PubMed abstracts (broad) + Europe PMC full-text (capped) ───
    all_docs: dict[str, Document] = {}
    fulltext_fetched = 0
    for q in QUERIES:
        if len(all_docs) >= PAPER_CAP:
            break
        pm_docs = await pm.fetch(query=q, limit=40)
        for d in pm_docs:
            key = d.metadata.get("pmid") or d.id
            if not key or key in processed or key in all_docs:
                continue
            if not RELEVANT.search(d.content[:3000]):
                continue
            all_docs[key] = d
            if len(all_docs) >= PAPER_CAP:
                break
        if fulltext_fetched < FULLTEXT_CAP:
            epmc_docs = await epmc.fetch(query=q, limit=12, full_text=True)
            for d in epmc_docs:
                key = d.metadata.get("pmid") or d.id
                if not RELEVANT.search(d.content[:3000]):
                    continue
                if key in all_docs and len(d.content) > len(all_docs[key].content):
                    all_docs[key] = d
                    if len(d.content) > 5000:
                        fulltext_fetched += 1
                elif key not in processed and key not in all_docs and len(all_docs) < PAPER_CAP:
                    all_docs[key] = d
                    if len(d.content) > 5000:
                        fulltext_fetched += 1
                if fulltext_fetched >= FULLTEXT_CAP:
                    break
        print(f"  {q[:44]:44s} (papers: {len(all_docs)}, ft: {fulltext_fetched})")

    docs = list(all_docs.values())[:PAPER_CAP]
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
