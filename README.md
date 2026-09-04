# graffold-ingest

Domain-agnostic knowledge graph ingestion. Extracts entities and relationships from any source, resolves them to canonical IDs, and publishes to your graph database of choice.

## Install

```bash
uv sync                    # core deps
uv sync --extra llm        # + LLM backends (boto3, openai)
uv sync --extra graph      # + community detection (graspologic)
uv sync --extra storage    # + analytics (duckdb)
uv sync --extra all        # everything
```

## Quick Start

```bash
# Run the full pipeline on a URL
graffold-ingest pipeline --source web --url "https://example.com" --service bedrock

# Start the API server
graffold-ingest serve --port 8001

# Push pre-extracted entities (from Agteria/Atlas)
curl -X POST http://localhost:8001/v1/entities \
  -H "Content-Type: application/json" \
  -d '{
    "source_run_id": "run-001",
    "source_system": "agteria",
    "entities": [{"id": "target:3-nop", "label": "Target", "name": "3-NOP"}],
    "relationships": [{"source": "target:3-nop", "target": "mech:mcr", "type": "INHIBITS"}]
  }'
```

## Architecture

```
Sources          Pipeline                    Storage            Query
─────────        ────────                    ───────            ─────
Web    ─┐                                 ┌─ Neo4j
PDF    ─┤  Fetch → Chunk → Extract →     │  Neptune (AWS)     DRIFT search
API    ─┼─ Resolve → Embed → Publish ──→ ├─ DuckDB (local)    Global search
CSV    ─┤           ↓                     │  Parquet (files)   Local search
DB     ─┤     Community detect            └─ (any GraphBackend)
Agteria─┘     (Leiden)
```

## Connectors

| Source | Fetches from |
|--------|-------------|
| `web` | URLs, sitemaps, crawl |
| `pdf` | Local PDFs |
| `api` | REST APIs with pagination |
| `csv` | CSV, Excel, Parquet files |
| `database` | SQL via connection string |

## Pipeline Stages

| Stage | What it does |
|-------|-------------|
| **Chunk** | Split documents into LLM-sized pieces |
| **Extract** | LLM discovers entities and relationships (schema-free) |
| **Resolve** | Deduplicate + canonicalize via UniProt/MONDO/PubChem |
| **Community** | Leiden clustering at multiple resolutions |
| **Publish** | MERGE to graph DB with provenance |
| **Embed** | Generate embeddings (CF Workers AI or local) |

## Search

| Mode | Algorithm | Use case |
|------|-----------|----------|
| **DRIFT** | Multi-hop iterative reasoning | Deep questions requiring graph traversal |
| **Global** | Map-reduce over community summaries | Broad questions about the whole corpus |
| **Local** | Vector + text search | Specific entity lookups |

## Storage Backends

Parquet is the source of truth. Backends are read/write adapters:

```bash
GRAPH_BACKEND=neo4j      # Default — Neo4j/Memgraph via Bolt
GRAPH_BACKEND=neptune    # AWS Neptune via OpenCypher + IAM
GRAPH_BACKEND=duckdb     # Local — queries Parquet via SQL
```

Configure via environment:
```bash
# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=secret

# Neptune
NEPTUNE_ENDPOINT=my-cluster.us-east-1.neptune.amazonaws.com
AWS_REGION=us-east-1

# Parquet output
PARQUET_DIR=~/.graffold/parquet
```

## API Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness check |
| `POST /v1/entities` | Push pre-extracted entities (from Agteria/Atlas) |
| `POST /ingest` | Full pipeline job (async) |
| `GET /jobs/{id}` | Job status |

## Use as a Library (SDK)

Everything the CLI does is available as a stable Python API. Import high-level
verbs and core types straight from the top-level package:

```python
import asyncio
from graffold_ingest import (
    Document, chunk_documents, extract_entities,
    EntityResolver, harmonize_graph,
    publish_to_parquet, read_parquet_graph,
    query_graph, get_backend,
)

async def main():
    # 1. Ingest
    docs = [Document(id="1", content="TP53 inhibits MDM2 in cancer.", source_type="text")]
    chunks = chunk_documents(docs, chunk_size=2000)
    results = await extract_entities(chunks, llm_service="bedrock-llama")

    # 2. Resolve + publish
    resolver = EntityResolver(enable_fuzzy=True)
    nodes = [n for r in results for n in r.nodes]
    edges = [e for r in results for e in r.edges]
    merged_n, merged_e = resolver.resolve(nodes, edges)
    from graffold_ingest import ExtractionResult
    await publish_to_parquet(
        [ExtractionResult(nodes=merged_n, edges=merged_e, source_doc_id="1")],
        output_dir="./graph",
    )

    # 3. Harmonize (collapse fragmented entities)
    n, e = read_parquet_graph("./graph", latest=True)
    n, e, report = harmonize_graph(n, e, use_embeddings=True)
    print(f"{report.entities_before} → {report.entities_after} entities")

    # 4. Query
    backend = get_backend("duckdb", parquet_dir="./graph")
    answer = await query_graph("What inhibits MDM2?", backend=backend)
    print(answer.answer)

asyncio.run(main())
```

**Public API** (stable, `from graffold_ingest import ...`):

| Symbol | Kind | Purpose |
|--------|------|---------|
| `Document`, `ExtractionResult` | types | Core data models |
| `chunk_documents`, `chunk_tabular` | func | Split text / CSV into chunks |
| `extract_entities`, `extract_entities_parallel` | async | LLM entity/relationship extraction |
| `EntityResolver` | class | 5-strategy resolution + fuzzy |
| `harmonize_graph`, `HarmonizeReport` | func/type | Collapse fragmented entities |
| `detect_communities`, `summarize_communities` | func | Leiden clustering + summaries |
| `publish_to_parquet`, `read_parquet_graph` | func | Storage (source of truth) |
| `get_backend`, `GraphBackend` | func/type | Neo4j / Neptune / Spanner / DuckDB |
| `query_graph`, `QueryResult` | async/type | Graph-grounded QA |
| `PubMedConnector`, `EuropePMCConnector` | class | Literature fetch |

Bare `import graffold_ingest` is lightweight — heavy deps (torch, boto3, pyarrow)
load lazily only when the symbol that needs them is used. Submodules remain
importable for advanced use; names prefixed with `_` are private.

For calling a *hosted* graffold service (rather than embedding the library),
use the REST API above or a thin typed client (see
`atlas/native/graffold_client.py` for the pattern).

## Entity Resolution

Built-in resolvers canonicalize entities against authoritative databases:

| Resolver | Handles | Authority |
|----------|---------|-----------|
| UniProt | Protein, Target, Enzyme | UniProt REST API |
| MONDO | Disease, Condition | EBI OLS4 |
| PubChem | Compound, Drug, Chemical | PubChem PUG REST |

## Integration

### With Agteria Platform

Agteria's Temporal workflow auto-publishes to graffold-ingest after each research run:
```python
# In publish_to_kg_activity:
await httpx_client.post(f"{GRAFFOLD_INGEST_URL}/v1/entities", json={...})
```

### With Atlas Pipeline

Atlas stages call graffold-api for validation and cross-run memory:
```python
from atlas.native.graffold_client import GraffoldClient
client = GraffoldClient()
await client.validate_targets(targets)
await client.find_similar_decisions("Kill GLP-2R")
```

## Development

```bash
uv sync --extra dev
pytest                     # 138 tests
ruff check src/            # lint
graffold-ingest tui        # interactive terminal UI
```

## Benchmarks

```bash
python benchmarks/ingest_throughput.py
python benchmarks/resolution_accuracy.py
python benchmarks/full_demo.py           # requires Ollama
```

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| Structured ingest | 180,000 entities/sec | CSV → Parquet |
| Tabular chunking | 920,000 rows/sec | CSV/TSV → markdown chunks |
| Entity resolution | 800,000 entities/sec | HGNC + synonym (no fuzzy) |
| Entity resolution | F1 = 1.00 | 9/9 true merges, 0 false merges |
| LLM extraction | ~6 entities/sec | Ollama qwen3:1.7b (local) |
| Query (local LLM) | 3–4 sec | Discovery → Expand → Synthesize |
| Storage | ~27 bytes/entity | Parquet (columnar, compressed) |

## License

AGPL-3.0 — see [LICENSE](LICENSE). Commercial licensing available (licensing@graffold.com).
