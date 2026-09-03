# ETEC Case Study — Full CLI Runbook

How we built a 1,000-paper knowledge graph for the post-weaning ETEC discovery
program, fused it with an Atlas reasoning run, harmonized it, and queried it for
feed-additive candidates — **entirely from the `graffold-ingest` CLI.**

Total spend: ~$8 (Llama 3.3 70B on Bedrock). Total wall time: ~1 hour.

---

## Prerequisites

```bash
# Install with all extras (LLM, graph, resolvers, storage)
uv sync --extra all

# LLM backend — one of:
export AWS_REGION=us-east-1          # Bedrock (used here: Llama 3.3 70B)
# or run Ollama locally for embeddings (harmonization uses nomic-embed-text)
ollama serve &
ollama pull nomic-embed-text

# Optional: faster PubMed
export NCBI_API_KEY=<your-key>
```

Model note: `--service bedrock-llama` uses `us.meta.llama3-3-70b-instruct-v1:0`
(~$0.72/M tokens, ~5× cheaper than Claude Haiku on output). Also supports
`bedrock`, `anthropic`, `openai`, `openrouter`, `ollama`.

---

## Step 1 — Seed the institutional backbone

The customer intake form (modality ranks, known dead-ends, constraints,
internal programs, untapped hypotheses) becomes the graph's backbone. This is
the one program-specific artifact — encoded as a small script since it *is*
customer data, not a reusable feature.

```bash
python benchmarks/etec_seed.py
# → 46 entities: 7 KILLED dead-ends, 3 internal programs,
#   5 constraints, 2 hypotheses, assays, targets
```

What it encodes (from the intake):
- **KILLED**: organic acids, carvacrol, EO blends, Bacillus blends, chelated
  zinc, insoluble fiber, generic gut-health — *never re-proposed*
- **Constraints**: feed-additive-only, pellet-stable-71°C, no vaccine, no small
  molecule
- **Hypotheses to test**: LTB+FedF combo, feed-borne enterotoxin binder
- **Internal programs** (differentiate, don't duplicate): GM Bacillus-FedF,
  soluble mannans, MCFA blends

---

## Step 2 — Ingest the literature corpus (1,000 papers)

One command runs fetch → extract → fuzzy-resolve → publish over 49 queries,
deduplicating papers across queries, then harmonizes the result.

```bash
graffold-ingest ingest-corpus \
  --queries benchmarks/etec-queries.txt \
  --source pubmed \
  --service bedrock-llama \
  --output ~/.graffold/parquet/etec-pigs \
  --per-query 40 \
  --paper-cap 1000 \
  --relevance "ETEC|F18|F4|enterotoxin|piglet|swine|porcine|fimbria|E\. ?coli" \
  --harmonize
```

- 49 queries span ETEC core + adjacent biology (adhesins, toxins, feed
  additives, host models, comparative pathogens)
- `--relevance` gate drops off-topic papers before extraction
- Checkpointed: re-run resumes, skipping processed papers
- `--harmonize` runs the canonicalization pass at the end

**Result:** 1,000 papers (207 full-text via Europe PMC) → 15,388 raw entities
→ 9,050 after fuzzy dedup. ~44 min, ~$5.

For full-text-primary (richer, costlier), swap `--source europepmc --full-text`.

---

## Step 3 — Ingest the Atlas reasoning run

Layer a real Atlas discovery run's decisions (candidates, kills, board
verdicts) on top of the literature. The `agteria` connector reads Atlas
`phase-*.md` files.

```bash
graffold-ingest ingest \
  ~/Developer/agteria/atlas/programs/phibro-etec-piglet-v2/v1 \
  --llm --service bedrock-llama
```

Or auto-watch a programs directory as Atlas runs finish:

```bash
graffold-ingest watch ~/Developer/agteria/atlas/programs --poll 60
```

**Result:** +1,197 entities, +2,690 relationships — 209 decision nodes fuse
with the literature (Atlas's "FedF"/"F18"/"IL-6" merge into the same nodes).

---

## Step 4 — Harmonize (collapse fragmented entities)

Literature fragments one entity across dozens of nodes ("F18 ETEC", "F18
fimbriae", "E. coli F18"). Harmonization collapses them into canonical nodes.

```bash
graffold-ingest harmonize ~/.graffold/parquet/etec-pigs
# → writes ~/.graffold/parquet/etec-pigs-harmonized
```

Layered for scientific safety:
1. **Alias rules** (deterministic) with type guard — a Target rule never
   swallows an InternalProgram
2. **Protected types** — Program/Hypothesis/Killed/Constraint never merged
3. **Embedding merge** (Ollama, 0.90 cosine) with a differing-code guard so
   F17 ≠ F18, IL-6 ≠ IL-10, STa ≠ STb

**Result:** 9,947 → 8,752 entities. F18-fimbriae 32→3, heat-labile 21→6
(remaining are real distinct subtypes).

---

## Step 5 — Query for candidates

Ask the graph for feed-additive candidates in the customer's ranked modalities:

```bash
graffold-ingest ask \
  "What feed-additive candidates target F18 or F4 adhesion or neutralize ETEC enterotoxins?" \
  --graph ~/.graffold/parquet/etec-pigs-harmonized
```

Generate a prior-knowledge document for the next Atlas run (the feedback loop):

```bash
graffold-ingest context "post-weaning ETEC" \
  --graph ~/.graffold/parquet/etec-pigs-harmonized \
  -o prior-knowledge.md
# Atlas run N+1 reads this at startup → won't re-propose carvacrol
```

Trace a specific target across all sources:

```bash
graffold-ingest trajectory "FedF" --graph ~/.graffold/parquet/etec-pigs-harmonized
```

---

## Step 6 — Inspect / export

```bash
# Graph stats
graffold-ingest status

# Export for Neo4j / Neptune / analysis
graffold-ingest export --format parquet -o etec-export.parquet
graffold-ingest export --format jsonl -o etec-graph.jsonl
```

The Parquet store is the source of truth. Point any backend at it:
```bash
GRAPH_BACKEND=neptune NEPTUNE_ENDPOINT=... graffold-ingest serve
GRAPH_BACKEND=spanner SPANNER_INSTANCE=... graffold-ingest serve
```

---

## Final graph

| Metric | Value |
|--------|------:|
| Papers | 1,000 (207 full-text) |
| Entities (harmonized) | 8,752 |
| Relationships | 20,150 |
| Evidence citations | 2,228 |
| Atlas decisions | 209 |
| Feed-additive candidates | 150 |
| Total LLM spend | ~$8 |
| Wall time | ~1 hour |

**Candidates surfaced** (ranked by graph connectivity): chicken egg yolk IgY,
bivalent F4/LT VHH construct, recombinant LTB, STb-binding molecule /
MBP-STb2 fusion, L. plantarum, Bacillus safensis M01, stapled/cyclised
peptides, berberine.

The graph independently surfaced the customer's own untapped hypotheses
(STb-binding, recombinant LTB) plus new candidates (egg yolk IgY, bivalent VHH)
— while the KILLED nodes block re-proposing the known dead-ends.

---

## Everything above is package code

Every step is a `graffold-ingest` CLI command backed by `src/graffold_ingest/`
package modules — not one-off scripts. The only script is `etec_seed.py`, which
encodes *this customer's* intake (program data, not a reusable feature).

| Step | Command | Module |
|------|---------|--------|
| Seed | `etec_seed.py` | (program data) |
| Literature | `ingest-corpus` | `connectors/pubmed.py`, `pipeline/extract.py`, `resolvers/local.py` |
| Atlas | `ingest` / `watch` | `connectors/agteria.py` |
| Harmonize | `harmonize` | `pipeline/harmonize.py` |
| Query | `ask` / `context` / `trajectory` | `pipeline/query_agent.py`, `query.py` |
| Export | `export` | `pipeline/export.py` |

---

## vs. Microsoft GraphRAG

GraphRAG (Microsoft Research, MIT) pioneered graph-based RAG. Graffold builds on
the same foundations (extraction, Leiden communities, local/global/DRIFT search,
Parquet) and adds what real discovery programs need.

| Capability | Microsoft GraphRAG | Graffold |
|------------|:------------------:|:--------:|
| Entity + relationship extraction | ✓ | ✓ |
| Leiden community detection | ✓ | ✓ |
| Local / Global / DRIFT search | ✓ | ✓ |
| Community summaries | ✓ | ✓ |
| Parquet output | ✓ | ✓ |
| Entity resolution | name-match only* | 5-strategy + fuzzy + HGNC/UniProt |
| Semantic harmonization | ✗ | ✓ (alias + embedding + code-guard) |
| Biomedical resolvers (UniProt/MONDO/PubChem) | ✗ | ✓ |
| Literature connectors (PubMed/Europe PMC) | ✗ | ✓ built-in |
| Graph backends | Azure-centric | Neo4j, Neptune, Spanner, DuckDB |
| Cross-run / institutional memory | ✗ | ✓ (kills, decisions, trajectories) |
| Decision capture (Atlas fusion) | ✗ | ✓ |
| Fact verification | ✗ | ✓ |
| License | MIT | AGPL-3.0 + commercial |

*GraphRAG deduplicates "by name (case-folded, whitespace-normalized)… no proper
entity resolution step beyond this" (Microsoft team + independent analysis). That
fragmentation is exactly what Graffold's harmonization solves — F18 collapsed
from 32 nodes to 3 in this run.
