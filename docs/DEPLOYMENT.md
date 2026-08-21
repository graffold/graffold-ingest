# Deployment

## Docker

```bash
docker build -t graffold-ingest .
docker run -p 8001:8001 \
  -e INGEST_INTERNAL_MODE=false \
  -e TENANT_KEYS="sk-prod:agteria" \
  -e GRAPH_BACKEND=neo4j \
  -e NEO4J_URI=bolt://neo4j:7687 \
  -e NEO4J_PASSWORD=secret \
  graffold-ingest
```

## Environment Variables

### Core

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8001` | API server port |
| `INGEST_INTERNAL_MODE` | `true` | Skip auth (dev only) |
| `TENANT_KEYS` | — | `key:tenant` pairs, comma-separated |
| `GRAPH_BACKEND` | `neo4j` | Backend: neo4j, neptune, duckdb |

### Neo4j

| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_URI` | `bolt://localhost:7687` | Bolt connection URI |
| `NEO4J_DATABASE` | `neo4j` | Database name |
| `NEO4J_USER` | `neo4j` | Username |
| `NEO4J_PASSWORD` | — | Password |

### AWS Neptune

| Variable | Default | Description |
|----------|---------|-------------|
| `NEPTUNE_ENDPOINT` | — | Cluster endpoint hostname |
| `NEPTUNE_PORT` | `8182` | Port |
| `AWS_REGION` | `us-east-1` | Region for SigV4 |

### Embeddings (Cloudflare)

| Variable | Default | Description |
|----------|---------|-------------|
| `CF_ACCOUNT_ID` | — | Cloudflare account |
| `CF_API_TOKEN` | — | Cloudflare API token |

### LLM

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama endpoint |

### Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `PARQUET_DIR` | `~/.graffold/parquet` | Parquet output directory |

## Publish Mode

The pipeline supports three publish modes:

```bash
# Neo4j only (default)
graffold-ingest pipeline --source web --url "..." --publish-mode neo4j

# Parquet only (no database required)
graffold-ingest pipeline --source web --url "..." --publish-mode parquet

# Both (transition period)
graffold-ingest pipeline --source web --url "..." --publish-mode dual
```

## Health Check

```bash
curl http://localhost:8001/health
# {"status": "ok"}
```

## CI/CD

The repo has GitHub Actions for auto-publish to PyPI on version tags:

```bash
git tag v0.2.0
git push --tags
# → publishes to PyPI automatically
```
