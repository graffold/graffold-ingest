"""Benchmark: entity resolution accuracy and speed."""

from __future__ import annotations

import time

from graffold_ingest.resolvers.local import EntityResolver


# Ground truth: pairs that SHOULD merge
SHOULD_MERGE = [
    ("RANTES", "CCL5"),
    ("MCP-1", "CCL2"),
    ("eotaxin", "CCL11"),
    ("FGF-basic", "FGF2"),
    ("FGF (basic)", "FGF2"),
    ("IP-10", "CXCL10"),
    ("SDF-1", "CXCL12"),
    ("tp53", "TP53"),  # case normalization
    ("gfap", "GFAP"),
]

# Pairs that should NOT merge
SHOULD_NOT_MERGE = [
    ("TP53", "BRCA1"),
    ("IL6", "IL8"),
    ("VEGFA", "VEGFB"),
    ("CCL5", "CXCL10"),
    ("JAK1", "JAK2"),
]


def bench_accuracy() -> dict:
    """Measure precision and recall of entity resolution."""
    resolver = EntityResolver(enable_fuzzy=False)

    # Test merges
    true_positives = 0
    false_negatives = 0
    for name_a, name_b in SHOULD_MERGE:
        nodes = [
            {"id": "a", "name": name_a, "type": "Protein"},
            {"id": "b", "name": name_b, "type": "Protein"},
        ]
        merged, _ = resolver.resolve(nodes, [])
        if len(merged) == 1:
            true_positives += 1
        else:
            false_negatives += 1
            print(f"    MISS: {name_a} ↔ {name_b} (not merged)")

    # Test non-merges
    true_negatives = 0
    false_positives = 0
    for name_a, name_b in SHOULD_NOT_MERGE:
        nodes = [
            {"id": "a", "name": name_a, "type": "Protein"},
            {"id": "b", "name": name_b, "type": "Protein"},
        ]
        merged, _ = resolver.resolve(nodes, [])
        if len(merged) == 2:
            true_negatives += 1
        else:
            false_positives += 1
            print(f"    FALSE MERGE: {name_a} ↔ {name_b}")

    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    return {
        "true_positives": true_positives,
        "false_negatives": false_negatives,
        "true_negatives": true_negatives,
        "false_positives": false_positives,
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "f1": round(f1, 3),
    }


def bench_speed(n_entities: int = 10000) -> dict:
    """Measure resolution speed."""
    resolver = EntityResolver(enable_fuzzy=False)

    nodes = [{"id": f"n{i}", "name": f"Protein_{i}", "type": "Protein"} for i in range(n_entities)]
    # Add some duplicates
    for i in range(0, n_entities, 10):
        nodes.append({"id": f"dup{i}", "name": f"protein_{i}", "type": "Protein"})

    rels = [{"source_id": f"n{i}", "target_id": f"n{i+1}", "type": "INTERACTS"} for i in range(n_entities - 1)]

    t0 = time.time()
    merged_n, merged_e = resolver.resolve(nodes, rels)
    elapsed = time.time() - t0

    return {
        "input_entities": len(nodes),
        "output_entities": len(merged_n),
        "merged": len(nodes) - len(merged_n),
        "seconds": round(elapsed, 3),
        "entities_per_sec": round(len(nodes) / elapsed),
    }


def main():
    print("=" * 50)
    print("  ENTITY RESOLUTION BENCHMARK")
    print("=" * 50)

    print("\n  Accuracy (synonym matching)...")
    acc = bench_accuracy()
    print(f"  → Precision: {acc['precision']}, Recall: {acc['recall']}, F1: {acc['f1']}")
    print(f"    TP={acc['true_positives']}, FN={acc['false_negatives']}, TN={acc['true_negatives']}, FP={acc['false_positives']}")

    print("\n  Speed (10,000 entities)...")
    speed = bench_speed(10000)
    print(f"  → {speed['entities_per_sec']:,} entities/sec, {speed['merged']} merged ({speed['seconds']}s)")

    print("\n  Speed (50,000 entities)...")
    speed = bench_speed(50000)
    print(f"  → {speed['entities_per_sec']:,} entities/sec, {speed['merged']} merged ({speed['seconds']}s)")


if __name__ == "__main__":
    main()
