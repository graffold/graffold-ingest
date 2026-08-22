"""Entity resolver — local synonym-based deduplication with fuzzy matching.

Resolution strategies (applied in order):
  1. Exact normalized match (case-insensitive, strip whitespace)
  2. Informal name map (~16 colloquial names not in any database)
  3. Abbreviation expansion (EGFR → Epidermal Growth Factor Receptor)
  4. Extended synonym map (HGNC aliases ~80K + UniProt gene/protein names ~50K)
  5. Fuzzy matching via rapidfuzz (Jaro-Winkler, threshold ≥ 0.92)

Ported from bioingest.pipeline.entity_resolver.
"""

from __future__ import annotations

import csv
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Strategy 2: colloquial biomarker names not in HGNC/UniProt
PROTEIN_SYNONYMS: dict[str, str] = {
    "rantes": "ccl5",
    "eotaxin": "ccl11",
    "gro-alpha": "cxcl1",
    "gro-α": "cxcl1",
    "mcp-1": "ccl2",
    "mip-1a": "ccl3",
    "mip-1α": "ccl3",
    "mip-1b": "ccl4",
    "mip-1β": "ccl4",
    "ip-10": "cxcl10",
    "sdf-1": "cxcl12",
    "sdf-1α": "cxcl12",
    "tarc": "ccl17",
    "mdc": "ccl22",
    "fgf-basic": "fgf2",
    "fgf (basic)": "fgf2",
}

# Lazy-loaded extended synonym map
_extended_synonyms: dict[str, str] | None = None


def _normalize(name: str) -> str:
    return name.strip().lower()


def _load_extended_synonyms(data_dir: Path | None = None) -> dict[str, str]:
    """Load HGNC aliases + UniProt gene/protein names (~80K+50K pairs).

    Auto-downloads HGNC on first use if missing (~5MB).
    Falls back gracefully if offline.
    """
    global _extended_synonyms
    if _extended_synonyms is not None:
        return _extended_synonyms

    _extended_synonyms = {}
    if data_dir is None:
        data_dir = Path(os.environ.get("GRAFFOLD_DATA_DIR", str(Path.home() / ".graffold" / "data")))

    # HGNC complete set
    hgnc_path = data_dir / "hgnc" / "hgnc_complete_set.tsv"
    if not hgnc_path.exists():
        _download_hgnc(hgnc_path)

    if hgnc_path.exists():
        try:
            with open(hgnc_path, encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    symbol = (row.get("symbol") or "").strip().lower()
                    if not symbol:
                        continue
                    for field in ("prev_symbol", "alias_symbol"):
                        raw = row.get(field, "")
                        if raw:
                            for alias in raw.replace('"', "").split("|"):
                                alias = alias.strip().lower()
                                if alias and alias != symbol:
                                    _extended_synonyms.setdefault(alias, symbol)
        except Exception:
            pass

    # UniProt SwissProt gene names
    tsv_path = data_dir / "uniprot" / "swissprot_tsv.tsv"
    if tsv_path.exists():
        try:
            with open(tsv_path, encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    gene_names = row.get("Gene Names", "")
                    if not gene_names:
                        continue
                    names = gene_names.split()
                    primary = names[0].lower()
                    for syn in names[1:]:
                        _extended_synonyms.setdefault(syn.lower(), primary)
        except Exception:
            pass

    return _extended_synonyms


def _download_hgnc(dest: Path) -> None:
    """Download HGNC complete set (~5MB). Silent failure if offline."""
    url = "https://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/tsv/hgnc_complete_set.txt"
    try:
        import urllib.request

        dest.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(url, dest)
    except Exception:
        pass  # ponytail: offline is fine, falls through to fuzzy-only


def _fuzzy_match(name: str, candidates: list[str], threshold: float = 0.92) -> str | None:
    """Best fuzzy match via rapidfuzz Jaro-Winkler. Returns key or None."""
    try:
        from rapidfuzz import fuzz, process

        result = process.extractOne(
            name, candidates, scorer=fuzz.WRatio, score_cutoff=threshold * 100
        )
        if result:
            return result[0]
    except ImportError:
        pass
    return None


class EntityResolver:
    """5-strategy entity resolver: exact → synonym → abbreviation → HGNC → fuzzy."""

    def __init__(
        self,
        synonym_map: dict[str, str] | None = None,
        data_dir: Path | None = None,
        enable_fuzzy: bool = True,
    ) -> None:
        self.synonym_map = {
            _normalize(k): _normalize(v)
            for k, v in (synonym_map or PROTEIN_SYNONYMS).items()
        }
        self.data_dir = data_dir
        self.enable_fuzzy = enable_fuzzy
        self._extended_loaded = False

    def resolve(
        self,
        nodes: list[dict[str, Any]],
        relationships: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Deduplicate nodes and remap relationship IDs.

        Returns (merged_nodes, remapped_relationships).
        """
        if not nodes:
            return nodes, relationships

        id_to_node: dict[str, dict[str, Any]] = {n["id"]: n for n in nodes}
        canon_map: dict[str, str] = {}  # node_id → canonical_id
        key_to_canonical_id: dict[str, str] = {}  # resolved_key → canonical node id

        for n in nodes:
            nid = n["id"]
            name = n.get("name", nid)
            norm = _normalize(name)
            key = norm

            # Strategy 2: synonym
            if norm in self.synonym_map:
                key = self.synonym_map[norm]

            # Strategy 3: abbreviation expansion
            if key not in key_to_canonical_id and key == norm:
                if name.strip().isupper() and len(name.strip()) <= 10:
                    for existing_key in list(key_to_canonical_id.keys()):
                        initials = "".join(w[0] for w in existing_key.split() if w)
                        if initials == norm:
                            key = existing_key
                            break

            # Strategy 4: extended HGNC + UniProt synonyms
            if key not in key_to_canonical_id and key == norm:
                if not self._extended_loaded:
                    ext = _load_extended_synonyms(self.data_dir)
                    for k, v in ext.items():
                        if k not in self.synonym_map:
                            self.synonym_map[k] = v
                    self._extended_loaded = True
                if norm in self.synonym_map:
                    candidate = self.synonym_map[norm]
                    if candidate in key_to_canonical_id:
                        key = candidate

            # Strategy 5: fuzzy matching
            if (
                key not in key_to_canonical_id
                and key == norm
                and self.enable_fuzzy
                and key_to_canonical_id
            ):
                match = _fuzzy_match(norm, list(key_to_canonical_id.keys()))
                if match:
                    key = match

            if key in key_to_canonical_id:
                canonical_id = key_to_canonical_id[key]
                # Keep longer name as display name
                existing = id_to_node[canonical_id]
                if len(name) > len(existing.get("name", existing["id"])):
                    existing["name"] = name
                canon_map[nid] = canonical_id
            else:
                key_to_canonical_id[key] = nid
                canon_map[nid] = nid

        # Deduplicated nodes
        seen: set[str] = set()
        merged_nodes: list[dict[str, Any]] = []
        for n in nodes:
            cid = canon_map[n["id"]]
            if cid not in seen:
                seen.add(cid)
                merged_nodes.append(id_to_node[cid])

        # Remap relationships
        seen_rels: set[tuple[str, str, str]] = set()
        remapped: list[dict[str, Any]] = []
        for r in relationships:
            src = canon_map.get(
                r.get("source_id", r.get("source", "")),
                r.get("source_id", r.get("source", "")),
            )
            tgt = canon_map.get(
                r.get("target_id", r.get("target", "")),
                r.get("target_id", r.get("target", "")),
            )
            rel_key = (src, tgt, r.get("type", ""))
            if rel_key not in seen_rels:
                seen_rels.add(rel_key)
                remapped.append({**r, "source_id": src, "target_id": tgt})

        logger.info(
            "Resolved %d → %d entities (%d merged)",
            len(nodes), len(merged_nodes), len(nodes) - len(merged_nodes),
        )
        return merged_nodes, remapped
