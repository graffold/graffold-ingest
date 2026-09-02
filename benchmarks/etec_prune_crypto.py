"""Prune crypto contamination from the ETEC graph.

The Atlas bakeoff directory mixed cryptosporidiosis content with ETEC.
The relevance gate caught most, but ~127 crypto entities leaked through.

This rewrites the graph (latest snapshot) excluding crypto-specific nodes
and any edges touching them. Append-only store → we write a fresh
consolidated snapshot to a clean dir.

Usage:
    python benchmarks/etec_prune_crypto.py
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from graffold_ingest.connectors.base import ExtractionResult
from graffold_ingest.pipeline.publish_parquet import publish_to_parquet, read_parquet_graph

SRC = Path.home() / ".graffold" / "parquet" / "etec-pigs"
DST = Path.home() / ".graffold" / "parquet" / "etec-pigs-clean"

# Crypto-specific terms — entities matching these are off-topic for ETEC/pigs
CRYPTO_TERMS = [
    "crypt", "cptrxr", "lapaquistat", "squalest", "cystamine", "pepstatin",
    "vorinostat", "cpcdpk", "cpasp", "cphdac", "oocyst", "sporozoite",
    "excystation", "apicomplex", "cp23", "fdft1", "bcl2a1", "glideosome",
    "myb-m", "t6ps", "t6pp", "niclosamide", "cpimpdh", "ndh2", "dhfr-ts",
    "v-atpase", "pfor", "rocaglate", "roc-a", "bez235", "ly2090314",
    "decoquinate", "auranofin", "halofuginone", "myoa", "cpeif4a",
    "parvum", "hominis", "coccidi", "eimeria", "toxoplasma",
]


def _is_crypto(entity: dict) -> bool:
    text = (entity.get("name", "") + " " + (entity.get("description") or "")).lower()
    return any(t in text for t in CRYPTO_TERMS)


async def main():
    nodes, edges = read_parquet_graph(SRC, latest=True)
    print(f"Source: {len(nodes)} entities, {len(edges)} relationships")

    # Identify crypto entity IDs
    crypto_ids = {n["id"] for n in nodes if _is_crypto(n)}
    print(f"Crypto-contaminated: {len(crypto_ids)} entities")

    # Keep non-crypto nodes
    clean_nodes = [n for n in nodes if n["id"] not in crypto_ids]

    # Keep edges only if BOTH endpoints survive
    clean_edges = [
        e for e in edges
        if e.get("source_id") not in crypto_ids
        and e.get("target_id") not in crypto_ids
    ]

    print(f"Clean: {len(clean_nodes)} entities, {len(clean_edges)} relationships")
    print(f"Removed: {len(nodes) - len(clean_nodes)} entities, {len(edges) - len(clean_edges)} relationships")

    # Write fresh consolidated snapshot
    if DST.exists():
        import shutil
        shutil.rmtree(DST)
    DST.mkdir(parents=True)

    result = ExtractionResult(nodes=clean_nodes, edges=clean_edges, source_doc_id="etec:consolidated")
    counts = await publish_to_parquet([result], output_dir=DST, run_id="consolidated-clean")
    print(f"\n\u2713 Clean graph written to {DST}")
    print(f"  {counts['entities_written']} entities, {counts['relationships_written']} relationships")


if __name__ == "__main__":
    asyncio.run(main())
