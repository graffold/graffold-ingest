"""Full end-to-end demo: ingest → resolve → publish → query.

Requires:
  - Ollama running with qwen3:1.7b and nomic-embed-text
  - graffold-ingest installed (pip install graffold-ingest or from git)

Usage:
  python run_demo.py
"""

from __future__ import annotations

import asyncio
import csv
import json
import time
from collections import Counter
from pathlib import Path

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)


async def main():
    from graffold_ingest.connectors.base import Document, ExtractionResult
    from graffold_ingest.pipeline.chunk import chunk_documents
    from graffold_ingest.pipeline.extract import extract_entities
    from graffold_ingest.pipeline.publish_parquet import publish_to_parquet, read_parquet_graph
    from graffold_ingest.pipeline.tabular import chunk_tabular
    from graffold_ingest.resolvers.local import EntityResolver
    from graffold_ingest.backends.duckdb import DuckDBBackend
    from graffold_ingest.pipeline.query_agent import query_graph

    parquet_dir = OUTPUT_DIR / "parquet"
    parquet_dir.mkdir(exist_ok=True)
    resolver = EntityResolver(enable_fuzzy=False)
    run_log: list[dict] = []

    print("=" * 70)
    print("  GRAFFOLD DEMO — Knowledge Graph Builder + Query Agent")
    print("=" * 70)

    # ─── Run 1: Structured data (sample proteins) ──────────────────────────
    print("\n▶ Run 1: Structured ingest (sample proteomics data)")
    t0 = time.time()

    sample_data = [
        ("P04637", "TP53", "Cellular tumor antigen p53", "Oncology"),
        ("P38398", "BRCA1", "Breast cancer type 1 susceptibility protein", "Oncology"),
        ("P05231", "IL6", "Interleukin-6", "Inflammation"),
        ("P01375", "TNF", "Tumor necrosis factor", "Inflammation"),
        ("P15692", "VEGFA", "Vascular endothelial growth factor A", "Oncology"),
        ("P10636", "MAPT", "Microtubule-associated protein tau", "Neurology"),
        ("P10145", "CXCL8", "Interleukin-8", "Inflammation"),
        ("Q15109", "AGER", "Advanced glycosylation end-product receptor", "Neurology"),
        ("P05067", "APP", "Amyloid-beta precursor protein", "Neurology"),
        ("O60674", "JAK2", "Tyrosine-protein kinase JAK2", "Oncology"),
    ]

    nodes, edges = [], []
    panels_seen = set()
    for uniprot, gene, protein, panel in sample_data:
        nodes.append({"id": f"uniprot:{uniprot}", "name": protein, "label": "Protein", "type": "Protein",
                      "properties": {"uniprot_id": uniprot, "gene_name": gene}})
        panel_id = f"panel:{panel.lower()}"
        if panel not in panels_seen:
            panels_seen.add(panel)
            nodes.append({"id": panel_id, "name": panel, "label": "Panel", "type": "Panel"})
        edges.append({"source_id": f"uniprot:{uniprot}", "target_id": panel_id, "type": "MEASURED_ON"})

    merged_n, merged_e = resolver.resolve(nodes, edges)
    results = [ExtractionResult(nodes=merged_n, edges=merged_e, source_doc_id="structured_run_1")]
    counts = await publish_to_parquet(results, output_dir=parquet_dir)
    elapsed = time.time() - t0

    run_log.append({"run": 1, "source": "structured", "entities": len(merged_n),
                    "relationships": len(merged_e), "seconds": round(elapsed, 2)})
    print(f"  ✓ {len(merged_n)} entities, {len(merged_e)} relationships ({elapsed:.1f}s)")

    # ─── Run 2: LLM extraction from abstracts ─────────────────────────────
    print("\n▶ Run 2: LLM extraction (3 PubMed abstracts via Ollama)")
    t0 = time.time()

    abstracts = [
        Document(id="pmid_ad", content="Tau protein aggregation hallmarks Alzheimer disease. p-tau217 and GFAP in CSF predict neurodegeneration. NFL correlates with amyloid burden.", source_type="pubmed"),
        Document(id="pmid_vegf", content="VEGFA drives tumor angiogenesis. Bevacizumab inhibits VEGFA. Resistance via FGF2. Faricimab dual-targets VEGFA and ANGPT2.", source_type="pubmed"),
        Document(id="pmid_jak", content="JAK2 V617F causes polycythemia vera. Ruxolitinib inhibits JAK1/JAK2. STAT3 drives BCL2 and MCL1 expression downstream.", source_type="pubmed"),
    ]

    llm_results = await extract_entities(abstracts, llm_service="ollama", model_id="qwen3:1.7b")
    all_n = [n for r in llm_results for n in r.nodes]
    all_e = [e for r in llm_results for e in r.edges]
    merged_n, merged_e = resolver.resolve(all_n, all_e)
    results = [ExtractionResult(nodes=merged_n, edges=merged_e, source_doc_id="pubmed_run_2")]
    counts = await publish_to_parquet(results, output_dir=parquet_dir)
    elapsed = time.time() - t0

    run_log.append({"run": 2, "source": "pubmed_llm", "entities": len(merged_n),
                    "relationships": len(merged_e), "seconds": round(elapsed, 2)})
    print(f"  ✓ {len(merged_n)} entities, {len(merged_e)} relationships ({elapsed:.1f}s)")

    # ─── Query demo ───────────────────────────────────────────────────────
    print("\n▶ Querying the graph...")
    backend = DuckDBBackend(parquet_dir=str(parquet_dir))

    questions = [
        "What drugs target VEGFA?",
        "What is tau protein associated with?",
        "Which proteins are on the Inflammation panel?",
    ]

    for q in questions:
        print(f"\n  Q: {q}")
        result = await query_graph(q, backend=backend, llm_service="ollama",
                                   llm_model="qwen3:1.7b", verify=False)
        # Truncate answer to 120 chars for display
        answer_preview = result.answer[:120] + "..." if len(result.answer) > 120 else result.answer
        print(f"  A: {answer_preview}")
        print(f"     ({result.total_seconds:.1f}s, {len(result.entities)} entities found)")

    # ─── Summary ──────────────────────────────────────────────────────────
    entities, relationships = read_parquet_graph(parquet_dir)
    print(f"\n{'═' * 70}")
    print(f"  FINAL GRAPH: {len(entities)} entities, {len(relationships)} relationships")
    print(f"  Storage: {sum(f.stat().st_size for f in parquet_dir.iterdir()) / 1024:.0f} KB")
    print(f"{'═' * 70}")

    # Save run log
    with open(OUTPUT_DIR / "run_log.json", "w") as f:
        json.dump(run_log, f, indent=2)
    print(f"\n  Run log saved: {OUTPUT_DIR / 'run_log.json'}")


if __name__ == "__main__":
    asyncio.run(main())
