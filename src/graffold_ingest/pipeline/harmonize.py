"""Graph harmonization — collapse fragmented entities into canonical nodes.

Fuzzy string dedup (during ingest) catches surface-similar names but misses
semantic duplicates that accumulate across papers: "F18 ETEC" / "F18 fimbriae"
/ "E. coli F18" are one entity fragmented 32 ways.

This runs a GLOBAL post-hoc pass over an assembled graph:

  1. Canonical alias rules (deterministic) — F18*, heat-labile*, STb*, etc.
  2. Same-type guard — never merge across entity types
  3. Embedding tiebreaker (conservative 0.88 cosine) — within-type only
  4. Remap all edges to canonical IDs, dedupe parallel edges

Alias rules are the safe backbone; embeddings only merge within-type pairs
that also share a token, above a high threshold. This avoids the
"FedF vs GM-Bacillus-expressing-FedF" false-merge trap.

Usage (as a module):
    from graffold_ingest.pipeline.harmonize import harmonize_graph
    nodes, edges, report = harmonize_graph(nodes, edges, use_embeddings=True)
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ─── Canonical alias rules ───────────────────────────────────────────────────
# Each rule: (compiled regex on normalized name) → canonical (id, display name, type)
# Order matters — first match wins. Keep specific before general.

_ALIAS_RULES: list[tuple[re.Pattern, str, str, str]] = [
    # Adhesins / fimbriae
    (re.compile(r"\bfedf\b"), "target:fedf", "FedF", "Target"),
    (re.compile(r"\bfaeg\b|k88.*faeg|faeg.*k88"), "target:faeg", "FaeG", "Target"),
    (re.compile(r"\bf18\b.*(fimbria|adhesin|etec|coli)|(?:^|\s)f18(?:ab|ac)?\s*(fimbria|adhesin)"),
     "pathogen:f18-etec", "F18 ETEC", "Organism"),
    (re.compile(r"\bf4\b.*(fimbria|adhesin|receptor)|k88.*fimbria"),
     "target:f4-fimbriae", "F4 fimbriae", "Target"),
    # Enterotoxins
    (re.compile(r"heat.?labile.*enterotoxin|heat.?labile.*(lt|toxin)|(?:^|\s)lt\s+enterotoxin|native heat.?labile"),
     "target:lt", "Heat-labile enterotoxin (LT)", "Target"),
    (re.compile(r"\bltb\b|lt b subunit|heat.?labile.*b subunit|eltb"),
     "target:ltb", "LTB (LT B subunit)", "Target"),
    (re.compile(r"heat.?stable.*\bb\b|\bstb\b|estb|heat.?stable enterotoxin b"),
     "target:stb", "Heat-stable enterotoxin b (STb)", "Target"),
    (re.compile(r"heat.?stable.*\ba\b|\bsta\b(?!\w)|esta|heat.?stable enterotoxin a"),
     "target:sta", "Heat-stable enterotoxin a (STa)", "Target"),
    (re.compile(r"heat.?stable.*enterotoxin|heat.?stable.*(st|toxin)"),
     "target:st", "Heat-stable enterotoxin (ST)", "Target"),
    (re.compile(r"shiga.*toxin|stx2e|stx"), "target:stx2e", "Shiga toxin (Stx2e)", "Target"),
    # Receptors / host
    (re.compile(r"gm1|ganglioside"), "target:gm1", "GM1 ganglioside", "Target"),
    (re.compile(r"tight.?junction"), "target:tight-junctions", "Tight junction proteins", "Target"),
    (re.compile(r"aminopeptidase n|\bapn\b"), "target:apn", "Aminopeptidase N (APN)", "Target"),
    (re.compile(r"guanylate cyclase|\bgc-?c\b"), "target:gcc", "Guanylate cyclase C", "Target"),
    # Pathogens
    (re.compile(r"enterotoxigenic.*coli|\betec\b"), "pathogen:etec", "Enterotoxigenic E. coli (ETEC)", "Organism"),
    # Cytokines (host inflammation)
    (re.compile(r"\bil-?6\b|interleukin.?6"), "target:il6", "IL-6", "Target"),
    (re.compile(r"\btnf'?\W?α?\b|tumor necrosis factor"), "target:tnf", "TNF-α", "Target"),
]


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.lower().strip())


# Institutional/seed types are hand-authored — never merged by alias rules.
_PROTECTED_TYPES = {
    "Program", "Hypothesis", "InternalProgram", "Constraint", "Modality",
    "Killed", "Assay", "Decision",
}

# Extraction-noise patterns — nodes matching these are document artifacts
# (truncated PMIDs, figure/table refs, meta-phrases), not real entities.
_NOISE_RE = re.compile(
    r"^(pmid|doi|ref|reference|fig(ure)?|table|supplementary|section|chapter)\b"
    r"|^(this|our|the|previous|future|prior|current)\s+(study|studies|research|"
    r"results?|data|work|authors?|paper|findings?|analysis|section|review|report)"
    r"|^et al\.?$|^\d{1,4}$|^[ivxlc]+$"  # bare numbers, roman numerals
    r"|amplicon-based|^unspecified|^n/?a$|^unknown$|^none$",
    re.IGNORECASE,
)


def _is_noise(node: dict) -> bool:
    name = (node.get("name") or "").strip()
    if not name or len(name) < 2:
        return True
    if node.get("type") in _PROTECTED_TYPES:
        return False  # never drop institutional nodes
    return bool(_NOISE_RE.search(name))


@dataclass
class HarmonizeReport:
    entities_before: int = 0
    entities_after: int = 0
    edges_before: int = 0
    edges_after: int = 0
    alias_merges: int = 0
    embedding_merges: int = 0
    noise_dropped: int = 0
    merge_examples: list[tuple[str, str]] = field(default_factory=list)


def _apply_alias_rules(
    nodes: list[dict],
) -> tuple[dict[str, str], dict[str, dict]]:
    """Map each node id → canonical id via alias rules.

    Returns (id_remap, canonical_nodes). Nodes matching no rule keep their own id.
    """
    id_remap: dict[str, str] = {}
    canonical: dict[str, dict] = {}

    for n in nodes:
        nid = n["id"]
        name = _norm(n.get("name", ""))
        ntype = n.get("type", n.get("label", ""))
        matched = None
        for pat, cid, cname, ctype in _ALIAS_RULES:
            if not pat.search(name):
                continue
            # Type guard: only merge if the node's type matches the rule's
            # canonical type. Protects Program/Hypothesis/InternalProgram/
            # Compound nodes from being swallowed by a Target/Organism rule.
            if ntype and ntype != ctype:
                continue
            # Protected types never merge via alias rules — they are
            # hand-authored institutional nodes, not literature fragments.
            if ntype in _PROTECTED_TYPES:
                continue
            matched = (cid, cname, ctype)
            break

        if matched:
            cid, cname, ctype = matched
            id_remap[nid] = cid
            if cid not in canonical:
                canonical[cid] = {
                    "id": cid, "name": cname, "type": ctype, "label": ctype,
                    "description": n.get("description", ""),
                }
            elif not canonical[cid].get("description") and n.get("description"):
                canonical[cid]["description"] = n["description"]
        else:
            id_remap[nid] = nid
            if nid not in canonical:
                canonical[nid] = dict(n)

    return id_remap, canonical


def _embedding_merge(
    canonical: dict[str, dict],
    id_remap: dict[str, str],
    threshold: float = 0.88,
) -> int:
    """Within-type embedding merge for nodes that also share a token.

    Conservative: only merges same-type pairs sharing at least one 3+ char
    token AND above the cosine threshold. Returns merge count.
    """
    try:
        from .embed_local import cosine_similarity, embed_single
    except Exception:
        return 0

    # Group canonical nodes by type
    by_type: dict[str, list[str]] = defaultdict(list)
    for cid, node in canonical.items():
        by_type[node.get("type", "?")].append(cid)

    def tokens(s: str) -> set[str]:
        return {t for t in re.findall(r"[a-z0-9]{3,}", s.lower())}

    def type_codes(s: str) -> set[str]:
        """Extract distinguishing alphanumeric codes (F18, F4, STa, IL-10, K88).

        Two names with DIFFERENT codes must not merge even if they embed close
        (F17 fimbriae != F18 fimbriae). Codes: letter(s)+digit(s) tokens.
        """
        return set(re.findall(r"\b[a-z]{1,4}\d{1,3}[a-z]?\b", s.lower()))

    merges = 0
    TARGET_TYPES = {"Target", "Compound", "Mechanism", "Disease", "Organism"}
    for t, ids in by_type.items():
        if t not in TARGET_TYPES or len(ids) < 2:
            continue
        ids = sorted(ids, key=lambda i: canonical[i].get("name", ""))
        vecs: dict[str, list[float]] = {}
        toks: dict[str, set[str]] = {}
        codes: dict[str, set[str]] = {}
        for cid in ids:
            nm = canonical[cid].get("name", "")
            toks[cid] = tokens(nm)
            codes[cid] = type_codes(nm)
        kept: list[str] = []
        for cid in ids:
            if cid in id_remap and id_remap[cid] != cid:
                continue
            merged = False
            for kid in kept:
                if not (toks[cid] & toks[kid]):
                    continue
                # Guard: if both have distinguishing codes and they DIFFER,
                # never merge (F17 vs F18, STa vs STb, IL6 vs IL10).
                if codes[cid] and codes[kid] and codes[cid] != codes[kid]:
                    continue
                if cid not in vecs:
                    vecs[cid] = embed_single(canonical[cid].get("name", ""))
                if kid not in vecs:
                    vecs[kid] = embed_single(canonical[kid].get("name", ""))
                if vecs[cid] and vecs[kid] and cosine_similarity(vecs[cid], vecs[kid]) >= threshold:
                    id_remap[cid] = kid
                    merges += 1
                    merged = True
                    break
            if not merged:
                kept.append(cid)

    return merges


def harmonize_graph(
    nodes: list[dict],
    edges: list[dict],
    *,
    use_embeddings: bool = True,
    embed_threshold: float = 0.90,
    drop_noise: bool = True,
) -> tuple[list[dict], list[dict], HarmonizeReport]:
    """Collapse fragmented entities into canonical nodes and remap edges.

    If drop_noise, extraction artifacts (truncated PMIDs, figure/table refs,
    meta-phrases like 'previous study') are removed before harmonization.

    Returns (canonical_nodes, remapped_edges, report).
    """
    report = HarmonizeReport(entities_before=len(nodes), edges_before=len(edges))

    # Pass 0: drop extraction noise
    if drop_noise:
        keep_ids = {n["id"] for n in nodes if not _is_noise(n)}
        report.noise_dropped = len(nodes) - len(keep_ids)
        nodes = [n for n in nodes if n["id"] in keep_ids]
        edges = [
            e for e in edges
            if e.get("source_id") in keep_ids and e.get("target_id") in keep_ids
        ]

    # Pass 1: deterministic alias rules
    id_remap, canonical = _apply_alias_rules(nodes)
    report.alias_merges = sum(1 for k, v in id_remap.items() if k != v)

    # Pass 2: embedding merge (within-type, conservative)
    if use_embeddings:
        report.embedding_merges = _embedding_merge(canonical, id_remap, embed_threshold)

    # Resolve remap chains (a→b→c) to final canonical
    def _final(cid: str) -> str:
        seen = set()
        while cid in id_remap and id_remap[cid] != cid and cid not in seen:
            seen.add(cid)
            cid = id_remap[cid]
        return cid

    final_remap = {n["id"]: _final(id_remap.get(n["id"], n["id"])) for n in nodes}

    # Build final node set (only surviving canonical ids)
    surviving = set(final_remap.values())
    final_nodes = [canonical[c] for c in surviving if c in canonical]

    # Collect a few merge examples for the report
    orig_names = {n["id"]: n.get("name", "") for n in nodes}
    for n in nodes:
        src, dst = n["id"], final_remap[n["id"]]
        if src != dst and len(report.merge_examples) < 15:
            report.merge_examples.append((orig_names.get(src, src), canonical.get(dst, {}).get("name", dst)))

    # Remap edges, dedupe parallel edges
    seen_edges: set[tuple] = set()
    final_edges: list[dict] = []
    for e in edges:
        s = final_remap.get(e.get("source_id", ""), e.get("source_id", ""))
        t = final_remap.get(e.get("target_id", ""), e.get("target_id", ""))
        if s == t:  # self-loop from merge
            continue
        key = (s, t, e.get("type", ""))
        if key in seen_edges:
            continue
        seen_edges.add(key)
        final_edges.append({**e, "source_id": s, "target_id": t})

    report.entities_after = len(final_nodes)
    report.edges_after = len(final_edges)
    logger.info(
        "Harmonized: %d→%d entities (%d alias, %d embedding), %d→%d edges",
        report.entities_before, report.entities_after,
        report.alias_merges, report.embedding_merges,
        report.edges_before, report.edges_after,
    )
    return final_nodes, final_edges, report
