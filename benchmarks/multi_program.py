"""Multi-program orchestrator + master cross-species merge.

Seeds each prospect program, (optionally) enriches with literature, harmonizes,
then unions all programs into a master graph — harmonized across programs so
shared entities (C. perfringens, sialidase, mucin) become single nodes linked
to multiple programs.

INTERNAL ONLY.

Usage:
    # seed all programs (fast, no LLM)
    python benchmarks/multi_program.py seed

    # enrich one program's literature (background)
    AWS_REGION=us-east-1 python benchmarks/multi_program.py enrich elanco-coccidiosis --papers 1000

    # merge all program graphs into master (cross-species harmonize)
    python benchmarks/multi_program.py merge
"""

from __future__ import annotations

import asyncio
import shutil
import sys
from pathlib import Path

from graffold_ingest.connectors.base import ExtractionResult
from graffold_ingest.pipeline.publish_parquet import publish_to_parquet, read_parquet_graph

sys.path.insert(0, str(Path(__file__).parent))
from programs import PROGRAMS  # noqa: E402

ROOT = Path.home() / ".graffold" / "parquet"


def _prog_dir(slug: str) -> Path:
    return ROOT / slug


async def seed_all() -> None:
    """Write the institutional seed for every program."""
    for slug, p in PROGRAMS.items():
        d = _prog_dir(slug)
        d.mkdir(parents=True, exist_ok=True)
        r = [ExtractionResult(nodes=p.seed_nodes, edges=p.seed_edges, source_doc_id=f"{slug}:seed")]
        c = await publish_to_parquet(r, output_dir=d, run_id="seed")
        print(f"  {slug:32s} seeded {c['entities_written']} entities, {c['relationships_written']} rels")


async def enrich(slug: str, papers: int, per_query: int = 40) -> None:
    """Fetch literature for a program, extract, resolve, publish, harmonize."""
    import json
    import re
    import time

    from graffold_ingest.connectors.pubmed import PubMedConnector
    from graffold_ingest.connectors.europepmc import EuropePMCConnector
    from graffold_ingest.connectors.base import Document
    from graffold_ingest.pipeline.chunk import chunk_documents
    from graffold_ingest.pipeline.extract import extract_entities_parallel
    from graffold_ingest.resolvers.local import EntityResolver

    p = PROGRAMS[slug]
    d = _prog_dir(slug)
    d.mkdir(parents=True, exist_ok=True)
    ckpt = d / ".enrich_checkpoint.json"
    processed = set(json.loads(ckpt.read_text()).get("processed", [])) if ckpt.exists() else set()

    pm, epmc = PubMedConnector(), EuropePMCConnector()
    resolver = EntityResolver(enable_fuzzy=True)
    print(f"[{slug}] enriching to {papers} papers ({len(p.queries)} queries)")

    all_docs: dict[str, Document] = {}
    fulltext = 0
    for q in p.queries:
        if len(all_docs) >= papers:
            break
        for d_ in await pm.fetch(query=q, limit=per_query):
            k = d_.metadata.get("pmid") or d_.id
            if k and k not in processed and k not in all_docs:
                all_docs[k] = d_
                if len(all_docs) >= papers:
                    break
        if fulltext < papers // 3:
            for d_ in await epmc.fetch(query=q, limit=10, full_text=True):
                k = d_.metadata.get("pmid") or d_.id
                if k in all_docs and len(d_.content) > len(all_docs[k].content):
                    all_docs[k] = d_
                    fulltext += 1
                elif k and k not in processed and k not in all_docs and len(all_docs) < papers:
                    all_docs[k] = d_
                    if len(d_.content) > 5000:
                        fulltext += 1
        print(f"  {q[:44]:44s} (papers: {len(all_docs)}, ft: {fulltext})")

    docs = list(all_docs.values())[:papers]
    chunks = chunk_documents(docs, chunk_size=4000)
    print(f"  {len(docs)} papers -> {len(chunks)} chunks")

    t0 = time.time()
    BATCH = 25
    consecutive_empty = 0
    for i in range(0, len(chunks), BATCH):
        batch = chunks[i:i + BATCH]
        results = await extract_entities_parallel(batch, llm_service="bedrock-llama", max_concurrent=6)
        results = [r for r in results if r and r.nodes]
        # Guard: a full batch of empty results usually means systemic failure
        # (expired AWS token, Bedrock throttle) — fail loud, don't checkpoint past it.
        if not results:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                raise RuntimeError(
                    f"{slug}: 3 consecutive empty batches at batch {i // BATCH + 1} "
                    f"— likely expired AWS token or Bedrock throttle. Aborting so the "
                    f"checkpoint doesn't skip these papers. Refresh auth and re-run."
                )
        else:
            consecutive_empty = 0
            nn = [n for r in results for n in r.nodes]
            ee = [e for r in results for e in r.edges]
            mn, me = resolver.resolve(nn, ee)
            await publish_to_parquet(
                [ExtractionResult(nodes=mn, edges=me, source_doc_id=f"{slug}:batch-{i // BATCH}")],
                output_dir=d, run_id=f"lit-{i // BATCH}")
            # only checkpoint papers we actually processed successfully
            for c_ in batch:
                processed.add(c_.id.split("_chunk")[0].replace("pmid:", ""))
            ckpt.write_text(json.dumps({"processed": sorted(processed)}))
        print(f"  batch {i // BATCH + 1}/{(len(chunks)+BATCH-1)//BATCH}: "
              f"+{sum(len(r.nodes) for r in results)} ent [{time.time()-t0:.0f}s]", flush=True)

    # harmonize
    from graffold_ingest.pipeline.harmonize import harmonize_graph
    n, e = read_parquet_graph(d, latest=True)
    fn, fe, rep = harmonize_graph(n, e, use_embeddings=True)
    hd = ROOT / f"{slug}-harmonized"
    if hd.exists():
        shutil.rmtree(hd)
    hd.mkdir(parents=True)
    await publish_to_parquet([ExtractionResult(nodes=fn, edges=fe, source_doc_id="harmonized")],
                             output_dir=hd, run_id="harmonized")
    print(f"[{slug}] done: {rep.entities_before} -> {rep.entities_after} entities in {(time.time()-t0)/60:.1f} min -> {hd}")


async def merge() -> None:
    """Union all program graphs into a master, harmonized cross-species.

    Tags every node with its source program(s) so shared entities show
    multi-program membership.
    """
    from graffold_ingest.pipeline.harmonize import harmonize_graph

    all_nodes: list[dict] = []
    all_edges: list[dict] = []
    program_of: dict[str, set[str]] = {}

    for slug in PROGRAMS:
        # prefer harmonized per-program graph, fall back to raw
        d = ROOT / f"{slug}-harmonized"
        if not (d / "entities.parquet").exists():
            d = _prog_dir(slug)
        if not (d / "entities.parquet").exists():
            print(f"  skip {slug} (no graph)")
            continue
        n, e = read_parquet_graph(d, latest=True)
        for node in n:
            program_of.setdefault(node["id"], set()).add(slug)
        all_nodes.extend(n)
        all_edges.extend(e)
        print(f"  loaded {slug}: {len(n)} entities, {len(e)} rels")

    print(f"\nMerging {len(all_nodes)} entity rows across {len(PROGRAMS)} programs...")
    fn, fe, rep = harmonize_graph(all_nodes, all_edges, use_embeddings=True)

    # annotate cross-program membership on surviving nodes
    # (harmonize collapses ids; recompute membership from merged names).
    # Store in source_doc_id since it persists in the Parquet schema.
    name_programs: dict[str, set[str]] = {}
    for node in all_nodes:
        name_programs.setdefault(node.get("name", "").lower(), set()).update(program_of.get(node["id"], set()))
    shared = 0
    for node in fn:
        progs = sorted(name_programs.get(node.get("name", "").lower(), set()))
        # description persists per-node; append a machine-readable membership tag
        base = (node.get("description") or "").split(" [programs:")[0]
        node["description"] = f"{base} [programs:{','.join(progs)}]".strip()
        if len(progs) > 1:
            shared += 1

    master = ROOT / "master"
    if master.exists():
        shutil.rmtree(master)
    master.mkdir(parents=True)
    await publish_to_parquet([ExtractionResult(nodes=fn, edges=fe, source_doc_id="master")],
                             output_dir=master, run_id="master")
    print(f"\nMASTER: {rep.entities_before} -> {rep.entities_after} entities, {rep.edges_after} rels")
    print(f"  Cross-program shared entities: {shared}")
    print(f"  -> {master}")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "seed"
    if cmd == "seed":
        asyncio.run(seed_all())
    elif cmd == "enrich":
        slug = sys.argv[2]
        papers = int(sys.argv[sys.argv.index("--papers") + 1]) if "--papers" in sys.argv else 1000
        asyncio.run(enrich(slug, papers))
    elif cmd == "merge":
        asyncio.run(merge())
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
