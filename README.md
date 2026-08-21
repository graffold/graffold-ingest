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
pytest                     # 55 tests
ruff check src/            # lint
graffold-ingest tui        # interactive terminal UI
```

## License

AGPL-3.0 — see [LICENSE](LICENSE). Commercial licensing available (licensing@graffold.com).
