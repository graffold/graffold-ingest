# Contributing

## Setup

```bash
git clone https://github.com/graffold/graffold-ingest.git
cd graffold-ingest
uv sync --extra dev --extra all
```

## Code Style

- Python 3.12+, type hints everywhere
- `ruff` for linting (88 line length, double quotes)
- Async/await for I/O-bound operations
- Pydantic models for API boundaries, dataclasses for internal state

## Testing

```bash
pytest                          # all tests
pytest tests/test_search.py     # specific file
pytest -k "drift"               # pattern match
```

Tests mock external services (Neo4j, LLM, HTTP APIs). No live services required.

## Adding a New Connector

1. Create `src/graffold_ingest/connectors/myconnector.py`
2. Implement the `Connector` protocol: `name()` and `async fetch(**kwargs) -> list[Document]`
3. Register in `connectors/__init__.py`

## Adding a New Backend

1. Create `src/graffold_ingest/backends/mybackend.py`
2. Implement the `GraphBackend` protocol
3. Add lazy-load in `backends/__init__.py::_load_builtin()`

## Adding a New Resolver

1. Create `src/graffold_ingest/resolvers/myresolver.py`
2. Extend `BaseResolver` with `resolve()` and `handles()`
3. Export in `resolvers/__init__.py`

## Project Structure

```
src/graffold_ingest/
├── api.py                  # FastAPI endpoints
├── cli.py                  # Click CLI
├── config.py               # Configuration
├── queue.py                # Async job queue
├── backends/               # Graph database adapters
│   ├── __init__.py         # GraphBackend protocol + registry
│   ├── neo4j.py
│   ├── neptune.py
│   └── duckdb.py
├── connectors/             # Data source fetchers
│   ├── web.py, pdf.py, api.py, csv.py, database.py
│   └── base.py             # Document model + Connector protocol
├── pipeline/               # Processing stages
│   ├── orchestrator.py     # Full pipeline runner
│   ├── chunk.py            # Document splitting
│   ├── extract.py          # LLM entity extraction
│   ├── resolve.py          # Name-based dedup
│   ├── community.py        # Leiden clustering
│   ├── publish.py          # Neo4j writer
│   ├── publish_parquet.py  # Parquet writer
│   ├── dual_write.py       # Multi-backend publish
│   ├── embed.py            # Embedding generation
│   ├── drift_search.py     # DRIFT multi-hop search
│   ├── global_search.py    # Map-reduce over communities
│   ├── entity_push.py      # Pre-extracted entity ingestion
│   └── nestboot_fdr.py     # Statistical edge confidence
└── resolvers/              # Entity canonicalization
    ├── base.py             # BaseResolver ABC
    ├── composite.py        # Chains resolvers by label
    ├── enhanced.py         # Two-pass resolution
    ├── uniprot.py, mondo.py, pubchem.py
    └── __init__.py
```
