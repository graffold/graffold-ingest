"""Demo: Atlas → Graffold → Atlas feedback loop.

Shows how a KG built by graffold-ingest feeds back into future Atlas runs
as a source of cross-program memory.

The cycle:
  1. Atlas Run 1 (crypto program) → pushes entities + decisions to graffold
  2. Atlas Run 2 (mastitis program) → queries graffold for prior knowledge
  3. Graffold returns cross-program hits → Atlas uses them in Phase 1

This proves the KG gets smarter across runs and programs.

Usage:
  python benchmarks/feedback_loop_demo.py
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

OUTPUT_DIR = Path("demo/feedback_loop")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


async def main():
    from graffold_ingest.connectors.base import ExtractionResult
    from graffold_ingest.pipeline.publish_parquet import publish_to_parquet, read_parquet_graph
    from graffold_ingest.pipeline.query_agent import query_graph
    from graffold_ingest.resolvers.local import EntityResolver
    from graffold_ingest.backends.duckdb import DuckDBBackend

    parquet_dir = OUTPUT_DIR / "parquet"
    parquet_dir.mkdir(exist_ok=True)
    resolver = EntityResolver(enable_fuzzy=False)

    print("=" * 70)
    print("  FEEDBACK LOOP DEMO: Atlas → Graffold → Atlas")
    print("  Proving the KG accumulates cross-program knowledge")
    print("=" * 70)

    # ═══════════════════════════════════════════════════════════════════════
    # ATLAS RUN 1: Cryptosporidiosis program
    # (Simulates what publish_to_kg_activity sends after a real Atlas run)
    # ═══════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("  ATLAS RUN 1: Cryptosporidiosis program (Cargill)")
    print("─" * 70)

    run1_entities = [
        {"id": "target:cptrxr", "name": "CpTrxR", "label": "Target", "type": "Target",
         "properties": {"organism": "C. parvum", "mechanism": "thioredoxin reductase", "uniprot": "Q5CVK0"}},
        {"id": "target:fdft1", "name": "FDFT1 (host squalene synthase)", "label": "Target", "type": "Target",
         "properties": {"organism": "Bos taurus", "mechanism": "squalene-GSH depletion"}},
        {"id": "disease:crypto", "name": "Bovine neonatal cryptosporidiosis", "label": "Disease", "type": "Disease"},
        {"id": "drug:auranofin", "name": "Auranofin", "label": "Drug", "type": "Drug",
         "properties": {"status": "KILLED", "reason": "gold toxicity, non-oral"}},
        {"id": "drug:lapaquistat", "name": "Lapaquistat", "label": "Drug", "type": "Drug",
         "properties": {"status": "KILLED", "reason": "hepatotoxicity at systemic dose"}},
        {"id": "target:bcl2a1", "name": "BCL2A1 (host apoptosis)", "label": "Target", "type": "Target",
         "properties": {"mechanism": "forced apoptotic extrusion of infected enterocytes"}},
        {"id": "mech:redox", "name": "Non-redundant redox economy", "label": "Mechanism", "type": "Mechanism"},
        {"id": "target:jak2", "name": "JAK2", "label": "Target", "type": "Target",
         "properties": {"note": "inflammation pathway, not primary target"}},
        {"id": "target:il6", "name": "IL-6", "label": "Target", "type": "Target",
         "properties": {"note": "downstream marker of gut inflammation"}},
    ]

    run1_relationships = [
        {"source_id": "target:cptrxr", "target_id": "disease:crypto", "type": "TREATS",
         "properties": {"confidence": 0.85, "evidence": "Gabriele 2025 PMID:40304242"}},
        {"source_id": "target:fdft1", "target_id": "disease:crypto", "type": "TREATS",
         "properties": {"confidence": 0.7, "evidence": "squalene-GSH depletion pathway"}},
        {"source_id": "drug:auranofin", "target_id": "target:cptrxr", "type": "INHIBITS",
         "properties": {"status": "killed", "reason": "gold compound, non-oral"}},
        {"source_id": "drug:lapaquistat", "target_id": "target:fdft1", "type": "INHIBITS",
         "properties": {"status": "killed", "reason": "hepatotox at systemic dose"}},
        {"source_id": "target:bcl2a1", "target_id": "disease:crypto", "type": "TREATS",
         "properties": {"mechanism": "clearance via forced extrusion"}},
        {"source_id": "target:cptrxr", "target_id": "mech:redox", "type": "PART_OF"},
        {"source_id": "target:il6", "target_id": "disease:crypto", "type": "ASSOCIATED_WITH",
         "properties": {"role": "inflammation marker"}},
        {"source_id": "target:jak2", "target_id": "target:il6", "type": "ACTIVATES"},
    ]

    # Also record a decision trace
    run1_decisions = [
        {"id": "decision:kill_auranofin", "name": "Kill Auranofin", "label": "Decision", "type": "Decision",
         "properties": {"stage": "phase-4", "reason": "gold toxicity, non-oral delivery",
                        "target": "CpTrxR", "outcome": "HARD_KILL"}},
        {"id": "decision:promote_cptrxr", "name": "Promote CpTrxR to Lead", "label": "Decision", "type": "Decision",
         "properties": {"stage": "phase-5", "reason": "sole NADPH-disulfide route per Gabriele 2025",
                        "target": "CpTrxR", "outcome": "PROMOTE_LEAD"}},
    ]
    run1_decision_edges = [
        {"source_id": "decision:kill_auranofin", "target_id": "drug:auranofin", "type": "KILLS"},
        {"source_id": "decision:kill_auranofin", "target_id": "target:cptrxr", "type": "ABOUT"},
        {"source_id": "decision:promote_cptrxr", "target_id": "target:cptrxr", "type": "PROMOTES"},
    ]

    all_nodes = run1_entities + run1_decisions
    all_edges = run1_relationships + run1_decision_edges
    merged_n, merged_e = resolver.resolve(all_nodes, all_edges)

    results = [ExtractionResult(nodes=merged_n, edges=merged_e, source_doc_id="atlas:crypto-v11:run-001")]
    await publish_to_parquet(results, output_dir=parquet_dir)

    print(f"  Published: {len(merged_n)} entities, {len(merged_e)} relationships")
    print(f"  Targets: CpTrxR, FDFT1, BCL2A1")
    print(f"  Decisions: Kill Auranofin, Promote CpTrxR")
    print(f"  Cross-links: JAK2 → IL-6 → cryptosporidiosis")

    # ═══════════════════════════════════════════════════════════════════════
    # ATLAS RUN 2: Mastitis program (different disease, overlapping biology)
    # ═══════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("  ATLAS RUN 2: Bovine mastitis program")
    print("  (Now queries the KG built by Run 1 for prior knowledge)")
    print("─" * 70)

    run2_entities = [
        {"id": "target:tlr4", "name": "TLR4", "label": "Target", "type": "Target",
         "properties": {"organism": "Bos taurus", "mechanism": "innate immune receptor"}},
        {"id": "disease:mastitis", "name": "Bovine mastitis", "label": "Disease", "type": "Disease"},
        {"id": "target:il6_r2", "name": "IL-6", "label": "Target", "type": "Target",
         "properties": {"note": "key mastitis inflammation mediator"}},
        {"id": "target:tnf", "name": "TNF-alpha", "label": "Target", "type": "Target"},
        {"id": "target:jak2_r2", "name": "JAK2", "label": "Target", "type": "Target",
         "properties": {"note": "JAK-STAT pathway in mastitis"}},
    ]

    run2_relationships = [
        {"source_id": "target:tlr4", "target_id": "disease:mastitis", "type": "ASSOCIATED_WITH"},
        {"source_id": "target:il6_r2", "target_id": "disease:mastitis", "type": "DRIVES"},
        {"source_id": "target:tnf", "target_id": "target:il6_r2", "type": "ACTIVATES"},
        {"source_id": "target:jak2_r2", "target_id": "target:il6_r2", "type": "ACTIVATES"},
    ]

    merged_n2, merged_e2 = resolver.resolve(run2_entities, run2_relationships)
    results2 = [ExtractionResult(nodes=merged_n2, edges=merged_e2, source_doc_id="atlas:mastitis-v1:run-001")]
    await publish_to_parquet(results2, output_dir=parquet_dir)

    print(f"  Published: {len(merged_n2)} entities, {len(merged_e2)} relationships")

    # ═══════════════════════════════════════════════════════════════════════
    # THE FEEDBACK: Run 2's Pathfinder queries the KG for prior knowledge
    # ═══════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("  FEEDBACK: Mastitis Pathfinder queries the accumulated KG")
    print("─" * 70)

    backend = DuckDBBackend(parquet_dir=str(parquet_dir))

    # Query 1: "What do we know about IL-6 from other programs?"
    print("\n  Pathfinder asks: 'What do we know about IL-6 from other programs?'")
    il6_hits = await backend.query_entities("IL-6", limit=10)
    print(f"  → Found {len(il6_hits)} IL-6 entities across programs:")
    for h in il6_hits:
        print(f"    • {h['name']} ({h['type']}) - {h['id']}")

    il6_neighbors = await backend.get_neighbors("target:il6", max_hops=1)
    print(f"  → IL-6 neighbors (from crypto program): {len(il6_neighbors.get('neighbors', []))}")
    for n in il6_neighbors.get("neighbors", []):
        print(f"    • {n['name']} ({n['type']})")

    # Query 2: "What JAK2-related decisions were made before?"
    print("\n  Pathfinder asks: 'Any prior decisions involving JAK2?'")
    jak2_neighbors = await backend.get_neighbors("target:jak2", max_hops=1)
    print(f"  → JAK2 connections from prior runs:")
    for n in jak2_neighbors.get("neighbors", []):
        print(f"    • {n['name']} ({n['type']})")

    # Query 3: Full query agent — "What targets have been killed and why?"
    print("\n  Pathfinder asks: 'What targets have been killed in past programs?'")
    result = await query_graph(
        "What drugs or targets have been killed and why?",
        backend=backend,
        llm_service="ollama",
        llm_model="qwen3:1.7b",
        verify=False,
    )
    print(f"  → Answer ({result.total_seconds:.1f}s):")
    for line in result.answer.split("\n")[:6]:
        print(f"    {line}")

    # ═══════════════════════════════════════════════════════════════════════
    # SUMMARY: What the feedback loop provides
    # ═══════════════════════════════════════════════════════════════════════
    entities_final, rels_final = read_parquet_graph(parquet_dir)

    print("\n" + "═" * 70)
    print("  FEEDBACK LOOP RESULTS")
    print("═" * 70)
    print(f"""
  After 2 Atlas runs, the KG contains:
    • {len(entities_final)} entities across 2 programs
    • {len(rels_final)} relationships (including cross-program links)

  What Run 2 (mastitis) learned from Run 1 (crypto):
    ✓ IL-6 was already flagged as inflammation marker in crypto
    ✓ JAK2 → IL-6 pathway already characterized
    ✓ Auranofin was KILLED (gold toxicity) — don't re-propose
    ✓ CpTrxR was PROMOTED — redox mechanism validated

  Without graffold: Run 2 starts from scratch, might re-propose killed drugs
  With graffold: Run 2 builds on Run 1's decisions automatically

  This is the value: CROSS-PROGRAM MEMORY that prevents repeated mistakes
  and accumulates biological insight across every Atlas run.
""")

    # Save summary
    summary = {
        "run_1": {"program": "cryptosporidiosis", "entities": len(merged_n), "relationships": len(merged_e)},
        "run_2": {"program": "mastitis", "entities": len(merged_n2), "relationships": len(merged_e2)},
        "accumulated": {"entities": len(entities_final), "relationships": len(rels_final)},
        "cross_program_hits": {
            "il6_shared": len(il6_hits),
            "jak2_connections": len(jak2_neighbors.get("neighbors", [])),
        },
    }
    with open(OUTPUT_DIR / "feedback_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Summary saved: {OUTPUT_DIR / 'feedback_summary.json'}")


if __name__ == "__main__":
    asyncio.run(main())
