"""Convert Neptune bulk-load CSVs (~id/~label/~from/~to) to a graffold Parquet graph.

The dump at demo/20260812_110637 is a Neptune Gremlin export:
  diseases.csv / proteins.csv / papers.csv  → nodes (~id,~label,name,...)
  edges.csv                                  → relationships (~id,~from,~to,~label,...)

~152K entities, 1.1M edges — the "massive" example. Maps into the same
entity/relationship Parquet the rest of the pipeline uses, so it flows through
catalog / merge / deploy unchanged.

Usage:
    python benchmarks/convert_neptune_dump.py demo/20260812_110637 --graph massive-ckd
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import sys
import time
from pathlib import Path

from graffold_ingest.connectors.base import ExtractionResult
from graffold_ingest.pipeline.publish_parquet import publish_to_parquet

# large edge file — bump csv field size
csv.field_size_limit(sys.maxsize)


def _load_nodes(path: Path) -> list[dict]:
    nodes = []
    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            nid = row.get("~id", "")
            if not nid:
                continue
            label = row.get("~label", "Entity")
            name = row.get("name:String") or row.get("doc_id:String") or nid
            desc_bits = []
            if row.get("publication_count:Int"):
                desc_bits.append(f"pubs={row['publication_count:Int']}")
            if row.get("normalized_name:String") and row["normalized_name:String"] != name:
                desc_bits.append(f"norm={row['normalized_name:String']}")
            nodes.append({
                "id": nid,
                "name": name,
                "label": label,
                "type": label,
                "description": " ".join(desc_bits),
            })
    return nodes


def _load_edges(path: Path) -> list[dict]:
    edges = []
    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            src, tgt = row.get("~from", ""), row.get("~to", "")
            if not src or not tgt:
                continue
            edges.append({
                "source_id": src,
                "target_id": tgt,
                "type": row.get("~label", "RELATED_TO"),
                "description": (row.get("description:String") or "")[:300],
            })
    return edges


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dump_dir")
    ap.add_argument("--graph", default="massive")
    ap.add_argument("--edge-cap", type=int, default=0, help="Cap edges (0=all)")
    args = ap.parse_args()

    dump = Path(args.dump_dir).expanduser()
    out = Path.home() / ".graffold" / "parquet" / args.graph
    out.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    nodes = []
    for fname in ("proteins.csv", "diseases.csv", "papers.csv"):
        fp = dump / fname
        if fp.exists():
            n = _load_nodes(fp)
            nodes.extend(n)
            print(f"  {fname}: {len(n):,} nodes")

    edges = _load_edges(dump / "edges.csv")
    if args.edge_cap:
        edges = edges[: args.edge_cap]
    print(f"  edges.csv: {len(edges):,} edges  [{time.time()-t0:.0f}s to parse]")

    # publish in chunks to keep memory bounded (1.1M edges)
    print("  writing Parquet...")
    NODE_BATCH = 50000
    for i in range(0, len(nodes), NODE_BATCH):
        await publish_to_parquet(
            [ExtractionResult(nodes=nodes[i:i + NODE_BATCH], edges=[], source_doc_id=f"neptune-dump:nodes-{i}")],
            output_dir=out, run_id="neptune-import")
    EDGE_BATCH = 100000
    for i in range(0, len(edges), EDGE_BATCH):
        await publish_to_parquet(
            [ExtractionResult(nodes=[], edges=edges[i:i + EDGE_BATCH], source_doc_id=f"neptune-dump:edges-{i}")],
            output_dir=out, run_id="neptune-import")
        print(f"    edges {min(i+EDGE_BATCH, len(edges)):,}/{len(edges):,}")

    print(f"\n  Done: {len(nodes):,} entities, {len(edges):,} rels -> {out}  [{(time.time()-t0)/60:.1f} min]")


if __name__ == "__main__":
    asyncio.run(main())
