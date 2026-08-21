# graffold-ingest ↔ Agteria Integration Handoff

> Prepared for: Agteria Platform integration (target: next month)
> Scope: This document covers the **ingestion pipeline** side only.
> For query/retrieval, see `graffold-api/AGTERIA_INTEGRATION_HANDOFF.md`.

---

## TL;DR

Agteria currently publishes extracted entities directly to Neo4j via its own `api.kg` module. The integration goal is to route entity ingestion **through graffold-ingest** so that deduplication, embedding, schema validation, and provenance are handled in one place. graffold-ingest already exposes a `POST /ingest` API with a job queue — Agteria needs a **structured entity push endpoint** that accepts pre-extracted entities (bypassing the LLM extraction stage).

---

## Architecture: Where graffold-ingest Fits

```
Agteria Research Workflow
│
│  publish_to_kg_activity (Temporal, fire-and-forget)
│  Currently: extract_from_abstract() → publish_to_neo4j() [direct]
│  Target:    extract_from_abstract() → POST graffold-ingest/v1/entities [via API]
│
▼
┌─────────────────────────────────────────────────────────────────────┐
│  graffold-ingest                                                     │
│                                                                       │
│  POST /v1/entities ─────► Validate ─► Resolve/Dedup ─► Publish      │
│       (new endpoint)       (schema)    (name matching)   (Neo4j)     │
│                                              │                        │
│                                              ▼                        │
│                                        Embed + Vectorize              │
│                                        (CF Workers AI → Vectorize)    │
│                                                                       │
│  POST /ingest ──────────► Fetch ─► Chunk ─► LLM Extract ─► ...      │
│       (existing)           (connectors)      (schema-free)            │
└─────────────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
                                    ┌─────────────────┐
                                    │     Neo4j       │
                                    │  + Vectorize    │
                                    └─────────────────┘
```

**Key distinction:** The existing `/ingest` endpoint runs the *full* pipeline (fetch → chunk → LLM extract → resolve → publish). Agteria already does its own extraction — it needs a **lighter endpoint** that accepts pre-extracted entities and only runs resolve → embed → publish.

---

## What Exists Today

### graffold-ingest API (`src/graffold_ingest/api.py`)

| Endpoint | Purpose | Auth |
|----------|---------|------|
| `GET /health` | Liveness | None |
| `POST /ingest` | Full pipeline job (source → KG) | Bearer token or internal mode |
| `GET /jobs/{id}` | Job status/result | Bearer token |

### Pipeline Stages (`src/graffold_ingest/pipeline/`)

| Stage | File | What it does |
|-------|------|--------------|
| Orchestrator | `orchestrator.py` | Runs stages sequentially with timing |
| Chunk | `chunk.py` | Splits documents into manageable pieces |
| Extract | `extract.py` | LLM-powered schema-free entity extraction |
| Resolve | `resolve.py` | Deduplicate entities by name (case-insensitive) |
| Dedup | `dedup.py` | Content-hash check against existing graph |
| Publish | `publish.py` | MERGE nodes/edges to Neo4j with provenance |
| Embed | `embed.py` | CF Workers AI → Vectorize upload |
| NestBoot FDR | `nestboot_fdr.py` | Bootstrap + shuffle for edge confidence |
| Export | `export.py` | DuckDB, Parquet, JSONL, TSV dumps |
| Schema | `schema.py` | YAML-based entity/relationship validation |

### Provenance Tracked (publish.py)

Every node and edge written carries:
```python
{
    "_source_doc_id": "...",      # Origin document
    "_ingested_at": 1720000000,   # Unix ms timestamp
    "_extraction_method": "llm",   # How it was extracted
    "_version_hash": "abc123...",  # Content fingerprint
}
```

---

## What Agteria Sends Today

From `agteria-platform/src/backend/services/api/kg/__init__.py`:

```python
@dataclass
class KGEntity:
    id: str            # "target:3-nop", "mechanism:mcr-inhibition"
    label: str         # "Target", "Mechanism", "Protein", "Publication"
    name: str          # Display name
    properties: dict   # modality, mechanism, uniprot_id, doi, etc.

@dataclass
class KGRelationship:
    source_id: str     # References KGEntity.id
    target_id: str
    type: str          # "HAS_MECHANISM", "TARGETS_PROTEIN", "MENTIONED_IN", etc.
    properties: dict   # source_run_id, etc.

@dataclass
class KGExtractionResult:
    entities: list[KGEntity]
    relationships: list[KGRelationship]
    source_run_id: str
    source_problem_id: str
```

Entity types extracted from Agteria research abstracts:
- **Target** — drug targets with modality, mechanism, rank
- **Mechanism** — biological processes
- **Protein** — with UniProt IDs, organism
- **Publication** — with DOI
- **ResearchTopic** — the program/problem being researched

Relationship types:
- `HAS_MECHANISM`, `TARGETS_PROTEIN`, `MENTIONED_IN`, `IDENTIFIES_TARGET`

### How Agteria Calls It (Current: Direct Neo4j)

```python
# In publish_to_kg_activity (Temporal activity, fire-and-forget):
extraction = extract_from_abstract(abstract_data, run_id, problem_id, partner_id)
await publish_to_neo4j(extraction)   # Direct Neo4j MERGE
await generate_embeddings()           # Ollama nomic-embed-text
```

---

## What Needs to Be Built

### Priority 1: `POST /v1/entities` — Structured Entity Push

Accept pre-extracted entities from Agteria without running the LLM extraction stage.

**Proposed request schema:**

```python
class EntityPushRequest(BaseModel):
    """Accept pre-extracted entities from an external system."""
    source_run_id: str                          # Agteria run UUID
    source_system: str = "agteria"              # Origin system identifier
    project_id: str = "default"                 # Tenant/project scope
    entities: list[EntityInput]
    relationships: list[RelationshipInput]

class EntityInput(BaseModel):
    id: str                                     # Stable external ID
    label: str                                  # Node label (Target, Mechanism, etc.)
    name: str                                   # Display name
    properties: dict[str, Any] = {}             # Arbitrary metadata

class RelationshipInput(BaseModel):
    source_id: str                              # References EntityInput.id
    target_id: str
    type: str                                   # Relationship type
    properties: dict[str, Any] = {}
```

**Proposed response:**

```python
class EntityPushResponse(BaseModel):
    job_id: str
    status: str                                 # "accepted" | "completed" (sync mode)
    nodes_created: int = 0
    nodes_merged: int = 0                       # Existing entities updated
    edges_created: int = 0
    embeddings_queued: int = 0
```

**Processing steps (in order):**
1. **Validate** — Check entities against schema (if configured)
2. **Resolve** — Deduplicate against existing graph by name
3. **Publish** — MERGE to Neo4j with provenance (`_extraction_method: "agteria"`)
4. **Embed** — Queue embedding generation (async, non-blocking)

**Sync vs Async:** For typical Agteria payloads (5–15 entities per run), run synchronously. For bulk imports (>50 entities), enqueue as a job and return `job_id`.

### Priority 2: Provenance Alignment

Agteria's provenance fields need to map cleanly to graffold-ingest's provenance model:

| Agteria field | graffold-ingest field | Notes |
|---------------|----------------------|-------|
| `source_run_id` | `_source_doc_id` | Treat run_id as the "document" |
| — | `_ingested_at` | Set by ingest at publish time |
| `"agteria"` | `_extraction_method` | New value (currently only "llm") |
| — | `_version_hash` | Hash of `source_run_id` |
| `source_problem_id` | `_source_problem_id` | New provenance field |
| `source_partner_id` | `_source_partner_id` | New provenance field |

### Priority 3: Entity Resolution Improvements

Current `resolve.py` does exact case-insensitive name matching. For Agteria integration:

1. **Cross-run dedup** — Same target discovered across multiple runs should merge:
   - `"3-NOP"` from run A == `"3-NOP (Bovaer)"` from run B
   - Needs fuzzy matching or alias support

2. **Mention counting** — Agteria already tracks `mention_count` on existing nodes via `ON MATCH`. graffold-ingest should increment similarly.

3. **ID stability** — Agteria generates deterministic IDs (`target:{slugify(name)}`). graffold-ingest should respect these as canonical when `source_system` provides them.

### Priority 4: Embedding Backend Alignment

| Context | Embedding model | Dimensions |
|---------|----------------|------------|
| graffold-ingest (CF) | `@cf/baai/bge-base-en-v1.5` | 768 |
| Agteria (local) | `nomic-embed-text` (Ollama) | 768 |
| graffold-ingest (Neo4j) | via `embed_and_upload()` | 768 |

Both are 768-dim, but **different models produce incompatible embedding spaces**. Decision needed:

- **Option A:** graffold-ingest always re-embeds (ignore Agteria embeddings) — simpler, consistent
- **Option B:** Accept pre-computed embeddings from Agteria — faster, but requires model alignment
- **Recommended:** Option A for now. graffold-ingest owns the embedding space.

---

## Integration Contract

### Environment Variables (Agteria side)

```bash
GRAFFOLD_ENABLED=true
GRAFFOLD_INGEST_URL=http://localhost:8001    # graffold-ingest service
GRAFFOLD_INGEST_API_KEY=sk-...               # Tenant key (maps to partner)
```

### Environment Variables (graffold-ingest side)

```bash
# Auth
TENANT_KEYS=sk-agteria-prod:agteria         # Maps API key → tenant
INGEST_INTERNAL_MODE=false                   # Require auth in production

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=...

# Embeddings
CF_ACCOUNT_ID=...
CF_API_TOKEN=...
```

### Agteria Migration Path

```python
# BEFORE (direct Neo4j publish in activities.py):
from api.kg import extract_from_abstract, publish_to_neo4j, generate_embeddings
extraction = extract_from_abstract(abstract_data, run_id, problem_id, partner_id)
await publish_to_neo4j(extraction)
await generate_embeddings()

# AFTER (via graffold-ingest API):
import httpx

ingest_url = os.getenv("GRAFFOLD_INGEST_URL", "http://localhost:8001")
ingest_key = os.getenv("GRAFFOLD_INGEST_API_KEY", "")

extraction = extract_from_abstract(abstract_data, run_id, problem_id, partner_id)

async with httpx.AsyncClient(timeout=30.0) as client:
    resp = await client.post(
        f"{ingest_url}/v1/entities",
        headers={"Authorization": f"Bearer {ingest_key}"},
        json={
            "source_run_id": run_id,
            "source_system": "agteria",
            "project_id": partner_id,
            "entities": [
                {"id": e.id, "label": e.label, "name": e.name, "properties": e.properties}
                for e in extraction.entities
            ],
            "relationships": [
                {"source_id": r.source_id, "target_id": r.target_id, "type": r.type, "properties": r.properties}
                for r in extraction.relationships
            ],
        },
    )
    resp.raise_for_status()
    result = resp.json()
    logger.info(f"KG publish via graffold-ingest: {result}")
```

---

## Neo4j Schema (Shared)

Both systems write to the same Neo4j instance. Index requirements:

```cypher
-- Vector index for semantic search (graffold-api uses this)
CREATE VECTOR INDEX entity_embeddings IF NOT EXISTS
FOR (e:Entity) ON e.embedding
OPTIONS {indexConfig: {`vector.dimensions`: 768, `vector.similarity_function`: 'cosine'}}

-- Fulltext for query routing coverage checks
CREATE FULLTEXT INDEX entity_names IF NOT EXISTS
FOR (e:Entity) ON EACH [e.name, e.description]

-- Lookup by stable ID
CREATE INDEX entity_id IF NOT EXISTS FOR (n:Entity) ON (n.id)

-- Provenance queries
CREATE INDEX source_doc IF NOT EXISTS FOR (n:Entity) ON (n._source_doc_id)
```

Node label strategy:
- Each entity gets its specific label (`Target`, `Mechanism`, `Protein`, etc.)
- **Plus** the generic `:Entity` label for vector/fulltext indexes
- graffold-ingest's `publish.py` already uses the specific label from extraction
- Agteria's `publish_to_neo4j()` adds `:Entity` as a secondary label

---

## Implementation Checklist

### Must-Have (Before Integration)

- [ ] **`POST /v1/entities`** — New endpoint accepting pre-extracted entities
- [ ] **Resolve against existing graph** — Cross-run dedup on entity push
- [ ] **Provenance for external sources** — `_extraction_method: "agteria"` + `_source_problem_id`
- [ ] **Sync response for small payloads** — Don't force Agteria to poll jobs for 10 entities
- [ ] **Schema validation passthrough** — Accept Agteria's entity labels without strict schema
- [ ] **Health check enhancement** — `GET /health` should report Neo4j connectivity

### Should-Have (First Month)

- [ ] **Bulk/batch mode** — Accept large entity sets (100+) as async jobs
- [ ] **Idempotency** — Re-pushing same `source_run_id` should be a no-op (not duplicates)
- [ ] **Alias resolution** — "3-NOP" == "3-NOP (Bovaer)" == "Bovaer"
- [ ] **Webhook callback** — Notify Agteria when async job completes
- [ ] **Metrics endpoint** — `GET /v1/stats` with entity counts, last-ingested timestamp

### Nice-to-Have (Later)

- [ ] **NestBoot FDR on relationship confidence** — Score edges from multiple runs
- [ ] **Cross-partner dedup** — Same target from different partners should merge
- [ ] **GNN validation** — Run sparselink on accumulated graph periodically
- [ ] **Export triggers** — Auto-export after N new entities (Parquet snapshots)

---

## Testing Plan

### Unit Tests (graffold-ingest)

```python
# tests/test_entity_push.py

async def test_entity_push_creates_nodes():
    """POST /v1/entities with valid payload creates nodes in graph."""

async def test_entity_push_deduplicates():
    """Same entity ID pushed twice → merge, not duplicate."""

async def test_entity_push_provenance():
    """Published nodes carry _extraction_method='agteria'."""

async def test_entity_push_embeds():
    """Pushed entities get embeddings generated."""

async def test_entity_push_auth_required():
    """Missing/invalid bearer token → 401/403."""
```

### Integration Test (End-to-End)

```bash
# 1. Start graffold-ingest
cd graffold-ingest && uvicorn graffold_ingest.api:app --port 8001

# 2. Push sample Agteria extraction
curl -X POST http://localhost:8001/v1/entities \
  -H "Authorization: Bearer sk-agteria-dev" \
  -H "Content-Type: application/json" \
  -d '{
    "source_run_id": "test-run-001",
    "source_system": "agteria",
    "project_id": "partner-cargill",
    "entities": [
      {"id": "target:3-nop", "label": "Target", "name": "3-NOP (Bovaer)", "properties": {"modality": "small_molecule"}},
      {"id": "mechanism:mcr-inhibition", "label": "Mechanism", "name": "MCR inhibition", "properties": {}}
    ],
    "relationships": [
      {"source_id": "target:3-nop", "target_id": "mechanism:mcr-inhibition", "type": "HAS_MECHANISM", "properties": {}}
    ]
  }'

# Expected response:
# {"job_id": "...", "status": "completed", "nodes_created": 2, "nodes_merged": 0, "edges_created": 1, "embeddings_queued": 2}

# 3. Verify in Neo4j
cypher-shell -u neo4j -p ... "MATCH (n:Target {id: 'target:3-nop'}) RETURN n"

# 4. Verify from graffold-api coverage check
curl -X POST http://localhost:8100/v1/coverage \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{"question": "What is 3-NOP?"}'
```

---

## Files to Modify

### graffold-ingest (this repo)

| File | Change |
|------|--------|
| `src/graffold_ingest/api.py` | Add `POST /v1/entities` endpoint |
| `src/graffold_ingest/pipeline/publish.py` | Support external provenance fields |
| `src/graffold_ingest/pipeline/resolve.py` | Cross-run resolution (query existing graph) |
| `src/graffold_ingest/pipeline/embed.py` | Async embedding after entity push |
| `tests/test_entity_push.py` | New test file |
| `pyproject.toml` | Add `neo4j` to dependencies |

### agteria-platform (their side)

| File | Change |
|------|--------|
| `src/backend/services/api/agent/activities.py` | Replace direct Neo4j with httpx call to graffold-ingest |
| `src/backend/services/api/kg/__init__.py` | Keep `extract_from_abstract()`, remove `publish_to_neo4j()` |
| `.env` | Add `GRAFFOLD_INGEST_URL`, `GRAFFOLD_INGEST_API_KEY` |

---

## Risks

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| graffold-ingest down → Agteria KG publish silently fails | Medium | Already fire-and-forget with 2 retries; add alerting |
| Embedding model mismatch corrupts vector index | High if ignored | graffold-ingest owns all embeddings (Option A) |
| Entity resolution too aggressive (false merges) | Low | Start conservative (exact name match), expand later |
| Latency added by routing through ingest API | Low | <100ms for typical payload; sync mode for small batches |
| Schema drift between Agteria entity types and graffold | Medium | Accept any label; schema validation is optional/soft |

---

## Decision Log

| Decision | Status | Notes |
|----------|--------|-------|
| graffold-ingest owns embedding space | Proposed | Agteria doesn't send embeddings; ingest generates them |
| Sync response for <50 entities | Proposed | Avoid polling overhead for typical payloads |
| Keep `extract_from_abstract()` in Agteria | Proposed | Agteria knows its own abstract structure best |
| Entity IDs are caller-provided (not generated) | Proposed | Agteria's `target:{slug}` format is stable and deterministic |
| Provenance uses `_extraction_method: "agteria"` | Proposed | Distinguishes from LLM-extracted entities |

---

## Quick Start (For the Implementing Developer)

```bash
cd /Users/apple/Developer/graffold/graffold-ingest

# 1. Activate env
source .venv/bin/activate

# 2. Run the existing API
INGEST_INTERNAL_MODE=true uvicorn graffold_ingest.api:app --port 8001 --reload

# 3. See what exists
curl http://localhost:8001/health
curl -X POST http://localhost:8001/ingest -H "Content-Type: application/json" \
  -d '{"source": "web", "url": "https://example.com"}'

# 4. Key files to read first:
#    - src/graffold_ingest/api.py          (add new endpoint here)
#    - src/graffold_ingest/pipeline/publish.py  (Neo4j write logic)
#    - src/graffold_ingest/pipeline/resolve.py  (dedup logic to enhance)
#    - src/graffold_ingest/pipeline/embed.py    (embedding pipeline)
```

---

**Summary:** graffold-ingest needs one new endpoint (`POST /v1/entities`) that accepts pre-extracted entities from Agteria, deduplicates them against the existing graph, publishes with provenance, and queues embeddings. Everything else (connectors, LLM extraction, chunking) remains for direct-source ingestion. The integration is minimal surface area — one endpoint, one migration on Agteria's side.
