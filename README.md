# graffold-ingest

Turn anything into a knowledge graph.

Domain-agnostic ingestion agent that scrapes, extracts, and publishes structured knowledge from any source — web pages, PDFs, APIs, CSVs, databases — into a Cypher-compatible graph database.

## Quick Start

```bash
uv sync
graffold-ingest tui        # Interactive terminal UI
graffold-ingest scrape     # Scrape a URL or file
graffold-ingest pipeline   # Run full ingestion pipeline
```

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Connectors │───▸│   Chunker   │───▸│  Extractor  │───▸│  Publisher  │
│ web/pdf/api │    │  split docs │    │ LLM entities│    │ graph write │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                            │
                                     ┌──────┴──────┐
                                     │  Resolver   │
                                     │ dedup/merge │
                                     └─────────────┘
```

## Connectors

| Connector | Sources | Status |
|-----------|---------|--------|
| `web` | Any URL, sitemaps, crawling | ✓ |
| `pdf` | Local PDFs, MarkItDown extraction | ✓ |
| `api` | REST APIs with pagination | ✓ |
| `csv` | CSV, Excel, Parquet files | ✓ |
| `database` | SQL databases via connection string | ✓ |

## Pipeline

```
graffold-ingest pipeline \
  --source web \
  --url "https://example.com/docs" \
  --database memgraph \
  --service bedrock
```

Pipeline stages:
1. **Fetch** — connector downloads/scrapes raw content
2. **Chunk** — split into manageable pieces
3. **Extract** — LLM discovers entities and relationships (schema-free)
4. **Resolve** — deduplicate and merge entities
5. **Embed** — generate vector embeddings
6. **Publish** — write nodes and edges to graph database

## Schema-Free Extraction

Unlike traditional NER pipelines, graffold-ingest doesn't require a predefined schema. The LLM discovers entity types and relationship types from the content itself:

```
Input: "Tesla CEO Elon Musk announced the Cybertruck will ship in Q4 2024"
Output:
  Nodes: (Person: Elon Musk), (Company: Tesla), (Product: Cybertruck)
  Edges: (Elon Musk)-[:CEO_OF]->(Tesla), (Tesla)-[:MANUFACTURES]->(Cybertruck)
```

## Works With

- **[graffold-api](https://github.com/graffold/graffold-api)** — Query the knowledge graph with natural language
- **[litecg](https://github.com/graffold/litecg)** — Context graph layer for decision traces
- **Any Cypher DB** — Neo4j, Memgraph, FalkorDB

## License

Apache 2.0

---

*By [Graffold](https://graffold.com)*
