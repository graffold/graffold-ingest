"""Benchmark: ingestion throughput (entities/sec)."""

from __future__ import annotations

import asyncio
import csv
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from graffold_ingest.connectors.base import ExtractionResult
from graffold_ingest.pipeline.publish_parquet import publish_to_parquet
from graffold_ingest.pipeline.tabular import chunk_tabular
from graffold_ingest.resolvers.local import EntityResolver


def generate_sample_csv(path: Path, n_rows: int = 1000) -> None:
    """Generate a sample proteomics CSV for benchmarking."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["uniprot_id", "gene_name", "protein_name", "panel"])
        for i in range(n_rows):
            writer.writerow([f"P{i:05d}", f"GENE{i}", f"Protein {i}", f"Panel_{i % 5}"])


async def bench_structured_ingest(n_entities: int = 5000) -> dict:
    """Benchmark structured CSV → Parquet throughput."""
    with TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "bench.csv"
        parquet_dir = Path(tmpdir) / "parquet"
        parquet_dir.mkdir()

        generate_sample_csv(csv_path, n_rows=n_entities)

        # Ingest
        t0 = time.time()

        nodes, edges = [], []
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                nodes.append({
                    "id": f"uniprot:{row['uniprot_id']}",
                    "name": row["protein_name"],
                    "label": "Protein",
                    "type": "Protein",
                })
                panel_id = f"panel:{row['panel'].lower()}"
                nodes.append({"id": panel_id, "name": row["panel"], "label": "Panel", "type": "Panel"})
                edges.append({"source_id": f"uniprot:{row['uniprot_id']}", "target_id": panel_id, "type": "MEASURED_ON"})

        resolver = EntityResolver(enable_fuzzy=False)
        merged_n, merged_e = resolver.resolve(nodes, edges)
        results = [ExtractionResult(nodes=merged_n, edges=merged_e, source_doc_id="bench")]
        await publish_to_parquet(results, output_dir=parquet_dir)

        elapsed = time.time() - t0
        throughput = len(merged_n) / elapsed

        return {
            "entities": len(merged_n),
            "relationships": len(merged_e),
            "seconds": round(elapsed, 3),
            "entities_per_sec": round(throughput),
        }


async def bench_tabular_chunking(n_rows: int = 10000) -> dict:
    """Benchmark tabular chunking speed."""
    with TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "big.csv"
        generate_sample_csv(csv_path, n_rows=n_rows)

        t0 = time.time()
        chunks = chunk_tabular(csv_path, rows_per_chunk=50)
        elapsed = time.time() - t0

        return {
            "rows": n_rows,
            "chunks": len(chunks),
            "seconds": round(elapsed, 3),
            "rows_per_sec": round(n_rows / elapsed),
        }


async def main():
    print("=" * 50)
    print("  INGEST THROUGHPUT BENCHMARK")
    print("=" * 50)

    print("\n  Structured ingest (5000 entities)...")
    r = await bench_structured_ingest(5000)
    print(f"  → {r['entities_per_sec']:,} entities/sec ({r['seconds']}s)")

    print("\n  Structured ingest (20000 entities)...")
    r = await bench_structured_ingest(20000)
    print(f"  → {r['entities_per_sec']:,} entities/sec ({r['seconds']}s)")

    print("\n  Tabular chunking (10000 rows)...")
    r = await bench_tabular_chunking(10000)
    print(f"  → {r['rows_per_sec']:,} rows/sec ({r['seconds']}s)")

    print("\n  Tabular chunking (50000 rows)...")
    r = await bench_tabular_chunking(50000)
    print(f"  → {r['rows_per_sec']:,} rows/sec ({r['seconds']}s)")


if __name__ == "__main__":
    asyncio.run(main())
