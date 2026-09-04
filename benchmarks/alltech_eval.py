"""Alltech gold-standard evaluation.

Compares a blinded literature-only graph against Alltech's expert intake.

Ground truth (from the Alltech intake form):
  - 11 pathogen-enzyme targets with UniProt IDs
  - ~40 named inhibitor compounds across 3 enzyme classes

Scenario B (guided): the blinded seed keeps enzyme CLASSES (sialidase/
chitinase/collagenase) + the aim, but strips the specific NanI/H/J proteins,
UniProt IDs, and named compounds. This harness measures whether 1000-paper
literature enrichment recovers them — and what NEW targets/evidence it adds.

Usage:
    python benchmarks/alltech_eval.py <graph-dir>
    # e.g. python benchmarks/alltech_eval.py ~/.graffold/parquet/alltech-blinded-harmonized
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from graffold_ingest.pipeline.publish_parquet import read_parquet_graph

# ─── Ground truth (Alltech intake) ───────────────────────────────────────────

GOLD_TARGETS = {
    # canonical name → accepted aliases (lowercased substrings)
    "C. perfringens sialidase NanI": ["nani", "sialidase nan"],
    "C. perfringens sialidase NanH": ["nanh"],
    "C. perfringens sialidase NanJ": ["nanj"],
    "C. perfringens collagenase": ["perfringens collagenase", "colg", "cola"],
    "C. perfringens chitinase": ["perfringens chitinase", "chia", "chib"],
    "E. coli chitinase ChiA": ["coli chitinase", "chia"],
    "Salmonella chitinase": ["salmonella chitinase"],
    "Salmonella collagenase": ["salmonella collagenase"],
    "Influenza neuraminidase": ["influenza neuraminidase", "viral neuraminidase", "influenza na"],
}

GOLD_ENZYME_CLASSES = ["sialidase", "neuraminidase", "chitinase", "collagenase"]

GOLD_COMPOUNDS = [
    "oseltamivir", "zanamivir", "peramivir", "siastatin", "siastain",
    "quercetin", "luteolin", "apigenin", "artocarpin", "diplacone",
    "kaempferol", "gossypetin", "katsumadain", "chromene",
    "allosamidin", "argifin", "demethylallosamidin", "caffeine",
    "theophylline", "pentoxifylline",
    "capsaicin", "curcumin", "dihydrorobinetin", "palmatine",
    "biochanin", "juglone", "chalcone",
]


def _norm(s: str) -> str:
    return s.lower().strip()


def evaluate(graph_dir: Path) -> None:
    nodes, edges = read_parquet_graph(graph_dir, latest=True)
    names = [_norm(n.get("name", "")) for n in nodes]
    blob = " | ".join(names)

    print("=" * 64)
    print(f"  ALLTECH GOLD-STANDARD EVAL  ({graph_dir.name})")
    print(f"  {len(nodes)} entities, {len(edges)} relationships")
    print("=" * 64)

    # ─── 1. Enzyme-class recall ───────────────────────────────────────────────
    print("\n[1] Enzyme classes (should all appear):")
    cls_hits = 0
    for c in GOLD_ENZYME_CLASSES:
        hit = c in blob
        cls_hits += hit
        print(f"    {'FOUND' if hit else 'MISS ':5s}  {c}")
    print(f"  → {cls_hits}/{len(GOLD_ENZYME_CLASSES)} enzyme classes recovered")

    # ─── 2. Specific target recall ────────────────────────────────────────────
    print("\n[2] Specific enzyme targets (Alltech's 9 canonical):")
    tgt_hits = 0
    for canon, aliases in GOLD_TARGETS.items():
        hit = any(a in blob for a in aliases)
        tgt_hits += hit
        print(f"    {'FOUND' if hit else 'MISS ':5s}  {canon}")
    print(f"  → {tgt_hits}/{len(GOLD_TARGETS)} specific targets recovered")

    # ─── 3. Compound recall ───────────────────────────────────────────────────
    print("\n[3] Named inhibitor compounds (Alltech listed ~27):")
    cpd_hits = [c for c in GOLD_COMPOUNDS if c in blob]
    print(f"    Recovered: {', '.join(sorted(cpd_hits)) or 'none'}")
    print(f"  → {len(cpd_hits)}/{len(GOLD_COMPOUNDS)} named compounds recovered")

    # ─── 4. NEW targets not in the gold set (augmentation) ────────────────────
    print("\n[4] Candidate NEW targets (enzymes/targets NOT in Alltech's list):")
    gold_terms = set()
    for aliases in GOLD_TARGETS.values():
        gold_terms.update(aliases)
    gold_terms.update(GOLD_ENZYME_CLASSES)
    new_targets = []
    for n in nodes:
        if n.get("type") not in ("Target", "Mechanism"):
            continue
        nm = _norm(n.get("name", ""))
        # enzyme-like but not in gold set
        if any(k in nm for k in ("ase", "inhibitor", "receptor", "transferase", "kinase")):
            if not any(g in nm for g in gold_terms):
                new_targets.append(n.get("name", ""))
    for t in sorted(set(new_targets))[:25]:
        print(f"    + {t[:55]}")
    print(f"  → {len(set(new_targets))} candidate new targets surfaced")

    # ─── 5. Evidence density ──────────────────────────────────────────────────
    evidence = [n for n in nodes if n.get("type") == "Evidence"]
    print(f"\n[5] Evidence base: {len(evidence)} citation nodes")
    print(f"    (PMIDs backing the graph's claims)")

    # ─── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 64)
    print("  SCORECARD (Scenario B — guided by enzyme classes)")
    print("=" * 64)
    print(f"  Enzyme-class recall:   {cls_hits}/{len(GOLD_ENZYME_CLASSES)}")
    print(f"  Specific-target recall:{tgt_hits}/{len(GOLD_TARGETS)}")
    print(f"  Compound recall:       {len(cpd_hits)}/{len(GOLD_COMPOUNDS)}")
    print(f"  New targets added:     {len(set(new_targets))}")
    print(f"  Evidence citations:    {len(evidence)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    evaluate(Path(sys.argv[1]).expanduser())
