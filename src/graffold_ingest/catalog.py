"""Graph catalog — inventory + browsable index for a Parquet graph store.

Scans a directory of graph subfolders, extracts stats (entities, relationships,
type breakdown, size, program membership), and emits:
  - a JSON manifest (machine-readable, for sync/display)
  - a Markdown or HTML index (human-browsable)

Usage:
    graffold-ingest catalog ~/.graffold/parquet
    graffold-ingest catalog ~/.graffold/parquet --format html -o catalog.html
    graffold-ingest catalog ~/.graffold/parquet --filter "alltech,etec,elanco,zoetis,master"
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class GraphEntry:
    name: str
    path: str
    entities: int = 0
    relationships: int = 0
    types: dict[str, int] = field(default_factory=dict)
    size_kb: int = 0
    programs: list[str] = field(default_factory=list)
    kind: str = "graph"  # graph | harmonized | clean | seed | atlas


def _classify(name: str) -> str:
    if name.endswith("-harmonized"):
        return "harmonized"
    if name.endswith("-clean"):
        return "clean"
    if name == "master":
        return "master"
    if "atlas" in name or "bakeoff" in name or "_analysis" in name:
        return "atlas"
    return "graph"


def scan(root: Path, filt: list[str] | None = None) -> list[GraphEntry]:
    """Scan a parquet store, return one GraphEntry per graph subfolder."""
    from collections import Counter

    import pyarrow.parquet as pq

    entries: list[GraphEntry] = []
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        ent_path = d / "entities.parquet"
        if not ent_path.exists():
            continue
        if filt and not any(f in d.name for f in filt):
            continue

        try:
            ent_tbl = pq.read_table(ent_path)
            rel_path = d / "relationships.parquet"
            rel_n = pq.read_metadata(rel_path).num_rows if rel_path.exists() else 0
            # dedup by id for a "latest" count
            ids = ent_tbl.column("id").to_pylist()
            types = ent_tbl.column("type").to_pylist()
            uniq = {}
            for i, t in zip(ids, types):
                uniq[i] = t
            type_counts = dict(Counter(uniq.values()).most_common())
            # program membership from description tags
            progs: set[str] = set()
            for desc in ent_tbl.column("description").to_pylist():
                m = re.search(r"\[programs:([^\]]+)\]", desc or "")
                if m:
                    progs.update(m.group(1).split(","))
            size_kb = sum(f.stat().st_size for f in d.iterdir() if f.is_file()) // 1024
            entries.append(GraphEntry(
                name=d.name, path=str(d), entities=len(uniq), relationships=rel_n,
                types=type_counts, size_kb=size_kb,
                programs=sorted(p for p in progs if p), kind=_classify(d.name),
            ))
        except Exception as e:
            print(f"  skip {d.name}: {e}")
    return entries


def to_manifest(entries: list[GraphEntry]) -> dict[str, Any]:
    return {
        "graphs": [asdict(e) for e in entries],
        "total_graphs": len(entries),
        "total_entities": sum(e.entities for e in entries),
        "total_relationships": sum(e.relationships for e in entries),
    }


def to_markdown(entries: list[GraphEntry]) -> str:
    lines = ["# Graph Catalog\n"]
    lines.append(f"{len(entries)} graphs · "
                 f"{sum(e.entities for e in entries):,} entities · "
                 f"{sum(e.relationships for e in entries):,} relationships\n")
    lines.append("| Graph | Kind | Entities | Rels | Size | Top types | Programs |")
    lines.append("|-------|------|---------:|-----:|-----:|-----------|----------|")
    for e in sorted(entries, key=lambda x: -x.entities):
        top = ", ".join(f"{k}:{v}" for k, v in list(e.types.items())[:3])
        progs = ", ".join(e.programs) if e.programs else "—"
        lines.append(f"| `{e.name}` | {e.kind} | {e.entities:,} | {e.relationships:,} | "
                     f"{e.size_kb}KB | {top} | {progs} |")
    return "\n".join(lines)


def to_html(entries: list[GraphEntry]) -> str:
    rows = ""
    for e in sorted(entries, key=lambda x: -x.entities):
        top = ", ".join(f"{k}:{v}" for k, v in list(e.types.items())[:4])
        progs = " ".join(f'<span class="tag">{p}</span>' for p in e.programs) or "—"
        kind_color = {"harmonized": "#34d399", "master": "#a78bfa", "clean": "#06b6d4",
                      "atlas": "#fbbf24", "graph": "#64748b"}.get(e.kind, "#64748b")
        rows += f"""<tr>
  <td><code>{e.name}</code></td>
  <td><span class="kind" style="background:{kind_color}22;color:{kind_color}">{e.kind}</span></td>
  <td class="num">{e.entities:,}</td>
  <td class="num">{e.relationships:,}</td>
  <td class="num">{e.size_kb} KB</td>
  <td class="types">{top}</td>
  <td>{progs}</td>
</tr>"""
    total_e = sum(e.entities for e in entries)
    total_r = sum(e.relationships for e in entries)
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Graffold Graph Catalog</title>
<style>
body{{font-family:system-ui,sans-serif;background:#0a0a0f;color:#fafafa;margin:0;padding:2rem}}
h1{{font-size:1.6rem}}.sub{{color:#a1a1aa;margin-bottom:1.5rem}}
table{{width:100%;border-collapse:collapse;font-size:.85rem}}
th,td{{padding:.5rem .75rem;text-align:left;border-bottom:1px solid #ffffff11}}
th{{color:#a1a1aa;text-transform:uppercase;font-size:.7rem;letter-spacing:.05em}}
.num{{text-align:right;font-variant-numeric:tabular-nums}}
code{{color:#34d399;font-size:.85em}}.types{{color:#a1a1aa;font-size:.8em}}
.kind{{padding:.15rem .5rem;border-radius:999px;font-size:.7rem;font-weight:600}}
.tag{{background:#a78bfa22;color:#a78bfa;padding:.1rem .4rem;border-radius:4px;font-size:.7rem;margin-right:.2rem}}
</style></head><body>
<h1>Graffold Graph Catalog</h1>
<p class="sub">{len(entries)} graphs · {total_e:,} entities · {total_r:,} relationships</p>
<table><thead><tr><th>Graph</th><th>Kind</th><th>Entities</th><th>Rels</th><th>Size</th><th>Top types</th><th>Programs</th></tr></thead>
<tbody>{rows}</tbody></table>
</body></html>"""
