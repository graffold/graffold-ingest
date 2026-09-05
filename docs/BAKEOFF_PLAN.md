# Plan: Serve graffold-ingest graphs to Atlas CLI via graffold-api (FalkorDB POC)

Status: **PLAN v2 — sharpened after decisions.** Iterate before building.

## Decisions (locked)

- **Backend:** FalkorDB only for the POC (its native multi-graph model fits
  "one clean set, per-program queries" perfectly — each program = a named
  graph in one instance). Neo4j/memgraph dropped; GCP graph is the future
  target, this is throwaway POC infra.
- **Graphs served:** one clean set — `etec`, `alltech`, `elanco`, `zoetis`,
  `master`. (Alltech blinded eval-variants excluded.)
- **Query granularity:** per-program (Atlas passes `kg_id`), plus `master`
  for cross-species. Graffold webapp lets a user pick which.
- **Deploy:** rsync Parquet to Hetzner + load on-box (ssh available), but the
  serving API should be reachable with just API keys.
- **Neptune:** out.

## The real scope (bigger than "load Parquet")

Two gaps, both needed:

### Gap A — FalkorDB loader in graffold-ingest  *(smaller)*
- Add `backends/falkordb.py` (Redis-protocol, batched `graph.query` UNWIND,
  mirrors the neptune writer). `get_backend("falkordb", graph_name=...)`.
- `graffold-ingest deploy <parquet-dir> --backend falkordb --graph <name>
  --host --port` → reads a Parquet graph, writes it to a named FalkorDB graph.

### Gap B — the `/v1/atlas/*` endpoints in graffold-api  *(the real work)*
The Atlas CLI's `graffold_client.py` calls **7 endpoints that don't exist yet**.
All take a `kg_id` (→ FalkorDB graph name). They must be built:

| Endpoint | Atlas expects returns | Backing query |
|----------|----------------------|---------------|
| `POST /v1/atlas/validate-targets` | `{validated, rejected, predicted_novel}` | entity presence + (optional) GNN score |
| `POST /v1/atlas/record-decision` | `{trace_id}` | write a Decision node |
| `GET  /v1/atlas/similar-decisions` | `{decisions:[...]}` | vector/text similarity over Decision nodes |
| `GET  /v1/atlas/entity-coverage` | `{total,found,coverage,missing}` | entity-id lookup |
| `GET  /v1/atlas/evidence-for-claim` | `{claim,evidence:[],contradiction_flag}` | S-P-O match + Evidence neighbors |
| `POST /v1/atlas/infer-network` | (network inference) | sparselink/GNN — may defer |
| `POST /v1/entities` (ingest) | push — **already exists in graffold-ingest** | — |

**Note:** graffold-api already routes per-graph via session
`connection_details.database` → FalkorDB `select_graph`. So `kg_id` → graph
name is a thin mapping, not new plumbing.

## Phases

### Phase 1 — FalkorDB loader (graffold-ingest)
`backends/falkordb.py` + `deploy` command. Local-testable against a local
FalkorDB container. **No API/infra dependency — start here.**

### Phase 2 — Local FalkorDB + load the clean set
`docker run falkordb`, load all 5 graphs as named graphs, verify with
`graffold-ingest ask --backend falkordb --graph alltech`.

### Phase 3 — `/v1/atlas/*` endpoints (graffold-api)
Build the 7 endpoints against FalkorDBDatabase, keyed by `kg_id` → graph name.
Start with the read endpoints (coverage, evidence, similar-decisions,
validate-targets); defer `infer-network` (needs GNN) and wire `record-decision`.

### Phase 4 — Hetzner deploy
- Add falkordb service to a compose file.
- rsync Parquet → box; `deploy` each graph on-box.
- Expose graffold-api behind API-key auth (not just Cloudflare Access) so the
  Atlas CLI can reach it with a key. **(needs the CF Access → API-key decision)**

### Phase 5 — end-to-end test with Atlas CLI
Point Atlas's `GRAFFOLD_API_URL` at Hetzner, `kg_id=alltech`, run its
validate-targets / evidence-for-claim calls against the real graph.

## Open questions

**Q-A — API-key auth vs Cloudflare Access.** You want Atlas to reach the API
"with just keys." Today `api.graffold.com` is behind Cloudflare Access (SSO
redirect). Options: (a) CF Access **service token** (header pair, works for
machine-to-machine — cleanest, no infra change), or (b) bypass CF for a
`/v1/atlas/*` path guarded by an API key. Recommendation: **CF service token**
— Atlas sends `CF-Access-Client-Id/Secret` headers + the existing API bearer.
Agree?

**Q-B — build order.** Phase 3 (the endpoints) is the actual product value and
is independent of Hetzner. Should I build **Phase 1 + 3 locally first** (loader
+ endpoints + a local FalkorDB), prove the Atlas CLI works end-to-end against
localhost, THEN do Hetzner (Phase 4)? That de-risks before touching prod.

**Q-C — `infer-network` / GNN.** Defer (return 501/empty) for the POC, or is
it needed for the Atlas CLI's first end-to-end? It's the one endpoint that
needs real ML (sparselink/PyKEEN), not just graph queries.

**Q-D — `validate-targets` GNN scoring.** Full version uses GNN link
validation. POC version could be presence + neighbor-degree heuristic
(no ML). OK to ship the heuristic first?
