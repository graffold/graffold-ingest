# Changelog

## 0.3.0

Major expansion: query, storage backends, harmonization, literature ingest.

### Added
- **Search** — DRIFT (multi-hop), Global (map-reduce over communities), and a
  5-phase graph-grounded query agent with fact verification.
- **Storage backends** — pluggable `GraphBackend` protocol with adapters for
  Neo4j (Bolt), AWS Neptune (boto3 neptunedata), Google Spanner Graph (GQL),
  and DuckDB (Parquet SQL). `GRAPH_BACKEND` env var selects.
- **Harmonization** (`pipeline/harmonize.py`, `graffold-ingest harmonize`) —
  global pass that collapses fragmented entities into canonical nodes via
  alias rules + within-type embedding merge + differing-code guard.
- **Literature connectors** — PubMed (NCBI E-utilities, rate-limited) and
  Europe PMC (abstracts + OA full-text) as native `fetch() → Document`.
- **Local resolver** (`resolvers/local.py`) — 5-strategy entity resolution
  (exact / synonym / abbreviation / HGNC+UniProt / rapidfuzz).
- **Community detection** — Leiden via graspologic-native + LLM summaries.
- **CLI** — `ingest-corpus` (whole-corpus literature ingest from a query list),
  `harmonize`, `query`, `ask`, `context`, `trajectory`, `watch`, `audit`,
  `init`, `status`. Added `pubmed`/`europepmc` sources + `bedrock-llama`
  service to `pipeline`.
- **Atlas integration** — `connectors/agteria.py` extracts phase-*.md; append-only
  Parquet with `run_id` for reproducible provenance; `read_parquet_graph(latest=True)`.
- **Local embeddings** (`pipeline/embed_local.py`) via Ollama nomic-embed-text.
- **Tabular chunker**, **section parser**, **NestBoot FDR**, **entity push API**.

### Changed
- Parquet is append-only (timestamped snapshots) instead of MERGE-overwrite.
- `neo4j` moved from core to `[graph]` optional dependency.

### Removed
- Dead code: `pipeline/vision.py`, `pipeline/node_labeler.py`, `SchemaStore`/
  `FingerprintStore` ABCs (single-implementation).

## 0.1.0
- Initial: domain-agnostic KG ingestion, connectors (web/pdf/api/csv/database),
  schema discovery, Neo4j publish, CLI + TUI.
