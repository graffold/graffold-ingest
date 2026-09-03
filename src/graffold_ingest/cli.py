"""graffold-ingest CLI."""

from __future__ import annotations

import asyncio

import click
from rich.console import Console

console = Console()


@click.group(invoke_without_command=True)
@click.version_option("0.2.0", prog_name="graffold-ingest")
@click.pass_context
def cli(ctx: click.Context) -> None:
    """graffold-ingest — Turn anything into a knowledge graph."""
    if ctx.invoked_subcommand is None:
        from .tui import main as tui_main

        tui_main()


@cli.command()
@click.argument("url")
@click.option("--service", default="bedrock", help="LLM service")
@click.option("--depth", default=1, type=int, help="Crawl depth")
def scrape(url: str, service: str, depth: int) -> None:
    """Scrape a URL and extract entities."""
    from .agent import scrape_agent

    console.print(f"[cyan]Scraping:[/] {url}")
    result = asyncio.run(scrape_agent(url, depth=depth, llm_service=service))
    console.print(f"[green]✓[/] {result}")


@cli.command()
@click.option("--source", type=click.Choice(["web", "pdf", "api", "csv", "database", "agteria", "pubmed", "europepmc"]), required=True)
@click.option("--url", default="")
@click.option("--path", default="")
@click.option("--query", default="", help="Search query (pubmed/europepmc)")
@click.option("--limit", default=25, type=int, help="Max papers to fetch (pubmed/europepmc)")
@click.option("--full-text/--abstract", default=True, help="Europe PMC: fetch OA full text")
@click.option("--service", default="bedrock")
@click.option("--database-uri", default="bolt://localhost:7687")
@click.option("--publish", "publish_mode", type=click.Choice(["neo4j", "parquet", "dual"]), default="parquet")
@click.option("--parquet-dir", default="", help="Parquet output directory")
@click.option("--resolve/--no-resolve", default=True, help="Resolve entities via UniProt/MONDO/PubChem")
@click.option("--direct", is_flag=True, help="Agteria: use regex extraction (skip LLM)")
def pipeline(source: str, url: str, path: str, query: str, limit: int, full_text: bool, service: str, database_uri: str, publish_mode: str, parquet_dir: str, resolve: bool, direct: bool) -> None:
    """Run the full ingestion pipeline."""
    from .connectors import CONNECTORS
    from .pipeline import (
        chunk_documents,
        extract_entities,
        publish_to_graph,
        resolve_entities,
    )

    console.print(f"[cyan]Pipeline:[/] {source} → extract → publish ({publish_mode})")

    async def _run():
        connector = CONNECTORS[source]()
        kwargs = {}
        if url:
            kwargs["url"] = url
        if path:
            kwargs["path"] = path
        if query:
            kwargs["query"] = query
            kwargs["limit"] = limit
        if source == "europepmc":
            kwargs["full_text"] = full_text

        # Agteria direct mode: regex extraction, no LLM
        if source == "agteria" and direct:
            from .connectors.agteria import AgteriaConnector
            from .pipeline.publish_parquet import DEFAULT_OUTPUT_DIR, publish_to_parquet

            results = await AgteriaConnector().extract_direct(**kwargs)
            total_nodes = sum(len(r.nodes) for r in results)
            total_edges = sum(len(r.edges) for r in results)
            console.print(f"  Extracted {total_nodes} nodes, {total_edges} edges (direct)")

            if resolve:
                try:
                    from .resolvers.composite import CompositeResolver
                    from .resolvers.mondo import MONDOResolver
                    from .resolvers.pubchem import PubChemResolver
                    from .resolvers.uniprot import UniProtResolver

                    resolver = CompositeResolver([UniProtResolver(), MONDOResolver(), PubChemResolver()])
                    resolved_count = 0
                    for r in results:
                        for node in r.nodes:
                            res = await resolver.resolve(node.get("name", ""), node.get("label", "").lower())
                            if res:
                                node["canonical_id"] = res.canonical_id
                                node["canonical_name"] = res.canonical_name
                                resolved_count += 1
                    console.print(f"  Resolved {resolved_count}/{total_nodes} entities")
                except ImportError:
                    pass

            out = parquet_dir or str(DEFAULT_OUTPUT_DIR)
            pq_stats = await publish_to_parquet(results, output_dir=out)
            console.print(f"  [green]✓[/] Parquet: {pq_stats}")
            return

        docs = await connector.fetch(**kwargs)
        console.print(f"  Fetched {len(docs)} documents")

        chunks = chunk_documents(docs)
        console.print(f"  Chunked into {len(chunks)} pieces")

        results = await extract_entities(chunks, llm_service=service)
        total_nodes = sum(len(r.nodes) for r in results)
        total_edges = sum(len(r.edges) for r in results)
        console.print(f"  Extracted {total_nodes} nodes, {total_edges} edges")

        # Literature sources use the fuzzy local resolver (collapses name variants)
        if source in ("pubmed", "europepmc") and resolve:
            from .resolvers.local import EntityResolver

            r = EntityResolver(enable_fuzzy=True)
            all_n = [n for res in results for n in res.nodes]
            all_e = [e for res in results for e in res.edges]
            merged_n, merged_e = r.resolve(all_n, all_e)
            from .connectors.base import ExtractionResult

            results = [ExtractionResult(nodes=merged_n, edges=merged_e, source_doc_id=f"{source}:{query[:40]}")]
            console.print(f"  Resolved {total_nodes} → {len(merged_n)} entities (fuzzy)")
        else:
            results = resolve_entities(results)
            console.print("  Resolved duplicates")

        if publish_mode in ("parquet", "dual"):
            from .pipeline.dual_write import publish_dual
            from .pipeline.publish_parquet import DEFAULT_OUTPUT_DIR, publish_to_parquet

            out = parquet_dir or str(DEFAULT_OUTPUT_DIR)
            if publish_mode == "dual":
                stats = await publish_dual(results, parquet_dir=out, database_uri=database_uri)
            else:
                stats = await publish_to_parquet(results, output_dir=out)
            console.print(f"  [green]✓[/] Published: {stats}")
        else:
            counts = await publish_to_graph(results, database_uri=database_uri)
            console.print(f"  [green]✓[/] Published: {counts}")

    asyncio.run(_run())


@cli.command()
def tui() -> None:
    """Launch the interactive terminal UI."""
    from .tui import main as tui_main

    tui_main()


@cli.command()
@click.argument("programs_dir", required=False)
@click.option("--parquet-dir", default="", help="Output directory (default: from config)")
@click.option("--resolve/--no-resolve", default=False, help="Resolve via UniProt/MONDO/PubChem")
@click.option("--poll", default=0, type=int, help="Poll interval seconds (0=run once)")
@click.option("--llm/--no-llm", default=False, help="Run LLM extraction (slower, richer)")
@click.option("--chunks", "-n", default=5, type=int, help="LLM chunks per program dir")
def watch(programs_dir: str | None, parquet_dir: str, resolve: bool, poll: int, llm: bool, chunks: int) -> None:
    """Watch Atlas programs/ directory and ingest new/changed phase outputs.

    Reads atlas_programs_dir from config if no argument given.
    Incremental: only processes directories with new or modified files.

    Examples:
        graffold-ingest watch
        graffold-ingest watch --poll 60
        graffold-ingest watch ~/atlas/programs/ --llm -n 10
    """
    import hashlib
    import json
    import os
    import re
    import time
    from pathlib import Path

    from .connectors.agteria import AgteriaConnector
    from .global_config import Config
    from .pipeline.publish_parquet import publish_to_parquet
    from .pipeline.resolve import resolve_entities as dedup_entities

    cfg = Config.load()

    # Resolve programs dir
    if programs_dir:
        programs_path = Path(programs_dir).expanduser().resolve()
    elif cfg.atlas_programs_dir:
        programs_path = Path(cfg.atlas_programs_dir)
    else:
        console.print("[red]No directory.[/] Pass a path or run `graffold-ingest init`.")
        return

    if not programs_path.is_dir():
        console.print(f"[red]Not a directory:[/] {programs_path}")
        return

    out_base = Path(parquet_dir) if parquet_dir else cfg.parquet_dir

    # Fingerprint store (incremental)
    fp_path = cfg.config_dir / "fingerprints.json"
    fingerprints: dict[str, str] = {}
    if fp_path.exists():
        try:
            fingerprints = json.loads(fp_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    def _fp(files: list[Path]) -> str:
        parts = sorted(f"{f.name}:{f.stat().st_size}:{int(f.stat().st_mtime)}" for f in files)
        return hashlib.md5("|".join(parts).encode()).hexdigest()[:12]

    def _save_fps():
        cfg.config_dir.mkdir(parents=True, exist_ok=True)
        fp_path.write_text(json.dumps(fingerprints))

    if llm and cfg.anthropic_api_key and not os.getenv("ANTHROPIC_API_KEY"):
        os.environ["ANTHROPIC_API_KEY"] = cfg.anthropic_api_key

    async def _ingest_once() -> int:
        connector = AgteriaConnector()
        processed = 0

        phase_files = list(programs_path.rglob("phase-*.md"))
        dirs: dict[Path, list[Path]] = {}
        for f in phase_files:
            dirs.setdefault(f.parent, []).append(f)

        for prog_dir, files in sorted(dirs.items()):
            rel = prog_dir.relative_to(programs_path)
            prog_name = str(rel).replace("/", "__")
            out_dir = out_base / prog_name

            # Incremental: skip unchanged
            fp = _fp(files)
            if fingerprints.get(prog_name) == fp:
                continue

            # Regex extract
            phase_names = [f.name for f in files]
            results = await connector.extract_direct(path=str(prog_dir), phases=phase_names)
            if not results or sum(len(r.nodes) for r in results) == 0:
                fingerprints[prog_name] = fp
                continue

            all_results = list(results)

            # LLM extract (optional)
            if llm and cfg.is_configured:
                from .connectors.base import ExtractionResult
                from .pipeline.chunk import chunk_documents
                from .pipeline.extract import EXTRACTION_PROMPT, _call_anthropic

                docs = await connector.fetch(path=str(prog_dir), phases=phase_names)
                all_chunks = chunk_documents(docs, chunk_size=4000)
                selected = all_chunks[:chunks]
                sem = asyncio.Semaphore(cfg.max_concurrent)

                async def _ex(text, cid):
                    async with sem:
                        try:
                            raw = await _call_anthropic(EXTRACTION_PROMPT.format(text=text[:8000]), cfg.llm_model)
                            cleaned = re.sub(r'```(?:json)?\s*\n?', '', raw).strip()
                            if cleaned.endswith('```'):
                                cleaned = cleaned[:-3].strip()
                            data = json.loads(cleaned)
                            return ExtractionResult(nodes=data.get('nodes',[]), edges=data.get('edges',[]), source_doc_id=cid)
                        except Exception:
                            return ExtractionResult(source_doc_id=cid)

                llm_res = await asyncio.gather(*[_ex(c.content, c.id) for c in selected])
                all_results.extend([r for r in llm_res if r.nodes])

            # Resolve (optional)
            if resolve:
                try:
                    from .resolvers.composite import CompositeResolver
                    from .resolvers.mondo import MONDOResolver
                    from .resolvers.pubchem import PubChemResolver
                    from .resolvers.uniprot import UniProtResolver
                    resolver = CompositeResolver([UniProtResolver(), MONDOResolver(), PubChemResolver()])
                    for r in all_results:
                        for node in r.nodes:
                            res = await resolver.resolve(node.get("name",""), (node.get("label") or node.get("type") or "").lower())
                            if res:
                                node["canonical_id"] = res.canonical_id
                                node["canonical_name"] = res.canonical_name
                except ImportError:
                    pass

            # Dedup + publish
            deduped = dedup_entities(all_results)
            out_dir.mkdir(parents=True, exist_ok=True)
            stats = await publish_to_parquet(deduped, output_dir=str(out_dir))
            written = stats.get("entities_written", 0)
            if written > 0:
                console.print(f"  [green]\u2713[/] {prog_name[:45]}: {written} ent, {stats.get('relationships_written',0)} rel")
                processed += 1

            fingerprints[prog_name] = fp

        _save_fps()
        return processed

    console.print(f"[cyan]Watching:[/] {programs_path}")
    if poll:
        console.print(f"[dim]Poll: {poll}s | LLM: {'on' if llm else 'off'} | Incremental[/]")

    while True:
        n = asyncio.run(_ingest_once())
        if not poll:
            if n == 0:
                console.print("[dim]Nothing new.[/]")
            break
        time.sleep(poll)

@cli.command()
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8000, type=int)
def serve(host: str, port: int) -> None:
    """Run the ingest API server."""
    import uvicorn

    uvicorn.run("graffold_ingest.api:app", host=host, port=port)


@cli.command()
@click.option("--format", "-f", type=click.Choice(["duckdb", "parquet", "jsonl", "tsv"]), default="duckdb")
@click.option("--output", "-o", default="graph_export.duckdb", help="Output file or directory")
@click.option("--database-uri", default="bolt://localhost:7687")
@click.option("--limit", default=0, type=int, help="Max nodes (0=all)")
def export(format: str, output: str, database_uri: str, limit: int) -> None:
    """Export the knowledge graph to DuckDB, Parquet, JSONL, or TSV."""
    from .pipeline.export import export_graph

    console.print(f"[cyan]Exporting:[/] {format} → {output}")
    stats = asyncio.run(export_graph(
        database_uri=database_uri,
        output=output,
        format=format,
        limit=limit,
    ))
    console.print(f"[green]✓[/] {stats['nodes']} nodes, {stats['edges']} edges → {output}")


@cli.group()
def schema() -> None:
    """Schema tools — discover, validate, refine your domain schema."""


@schema.command()
@click.option("--domain", "-d", default="", help="Domain description in plain English")
@click.option("--from-file", "from_file", default="", help="Sample file to analyze")
@click.option("--from-url", "from_url", default="", help="URL to analyze")
@click.option("--service", default="bedrock", help="LLM service")
@click.option("--output", "-o", default="schema.yaml", help="Output file")
@click.option("--tenant", default="default", help="Tenant ID")
@click.option("--project", default="default", help="Project ID")
def discover(domain: str, from_file: str, from_url: str, service: str, output: str, tenant: str, project: str) -> None:
    """Discover a schema from sample data or a domain description."""
    from pathlib import Path

    from .pipeline.discover import discover_schema, save_schema, validate_schema
    from .schema_store import FileSchemaStore

    content = ""
    if from_file:
        content = Path(from_file).read_text(errors="ignore")[:8000]
        console.print(f"[cyan]Analyzing:[/] {from_file} ({len(content)} chars)")
    elif from_url:
        import httpx

        content = httpx.get(from_url, timeout=30).text[:8000]
        console.print(f"[cyan]Analyzing:[/] {from_url}")
    elif domain:
        content = domain
        console.print(f"[cyan]Domain:[/] {domain}")
    else:
        console.print("[red]Provide --domain, --from-file, or --from-url[/]")
        return

    console.print("[dim]Generating schema with LLM...[/]")
    yaml_content = asyncio.run(discover_schema(content=content, llm_service=service))

    issues = validate_schema(yaml_content)
    if issues:
        console.print(f"[yellow]⚠ Schema has {len(issues)} issue(s):[/]")
        for issue in issues:
            console.print(f"  • {issue}")
    else:
        console.print("[green]✓[/] Schema is valid")

    save_schema(yaml_content, output)
    console.print(f"[green]✓[/] Saved to {output}")

    # Persist version
    store = FileSchemaStore(Path.home() / ".graffold" / "schemas")
    v = store.save(tenant, project, yaml_content, description=f"discover: {domain or from_file or from_url}")
    console.print(f"[dim]Version {v.version_id[:8]} saved[/]")
    console.print(f"\n[dim]Preview:[/]\n{yaml_content[:500]}")


@schema.command()
@click.argument("path", default="schema.yaml")
def validate(path: str) -> None:
    """Validate a schema YAML file."""
    from pathlib import Path

    from .pipeline.discover import validate_schema

    content = Path(path).read_text()
    issues = validate_schema(content)
    if issues:
        console.print(f"[red]✗[/] {len(issues)} issue(s) in {path}:")
        for issue in issues:
            console.print(f"  • {issue}")
    else:
        console.print(f"[green]✓[/] {path} is valid")

        from .pipeline.schema import KGSchema

        s = KGSchema.load(path)
        console.print(f"  {len(s.entities)} entity types, {len(s.relationships)} relationship types")


@schema.command()
@click.argument("path", default="schema.yaml")
@click.option("--feedback", "-f", required=True, help="What to change")
@click.option("--service", default="bedrock")
@click.option("--tenant", default="default", help="Tenant ID")
@click.option("--project", default="default", help="Project ID")
def refine(path: str, feedback: str, service: str, tenant: str, project: str) -> None:
    """Refine an existing schema based on feedback."""
    from pathlib import Path

    from .pipeline.discover import refine_schema, save_schema, validate_schema
    from .schema_store import FileSchemaStore

    current = Path(path).read_text()
    console.print(f"[cyan]Refining:[/] {path}")
    console.print(f"[dim]Feedback:[/] {feedback}")

    updated = asyncio.run(refine_schema(current, feedback, llm_service=service))

    issues = validate_schema(updated)
    if issues:
        console.print(f"[yellow]⚠ {len(issues)} issue(s) — saving anyway[/]")

    save_schema(updated, path)
    console.print(f"[green]✓[/] Updated {path}")

    # Persist version
    store = FileSchemaStore(Path.home() / ".graffold" / "schemas")
    v = store.save(tenant, project, updated, description=f"refine: {feedback[:60]}")
    console.print(f"[dim]Version {v.version_id[:8]} saved[/]")


@cli.command()
@click.argument("program_dir")
@click.option("--graph", default="~/.graffold/parquet/atlas-full", help="Parquet graph directory")
@click.option("--output", "-o", default="", help="Output markdown file (default: print to stdout)")
def audit(program_dir: str, graph: str, output: str) -> None:
    """Run graph-powered audit on an Atlas program directory.

    Checks kill consistency, evidence coverage, omissions, version drift,
    and novel connections against the accumulated knowledge graph.

    Examples:
        graffold-ingest audit ~/atlas/programs/crypto-v11/v1/
        graffold-ingest audit ./programs/my-prog/v1/ --graph ~/.graffold/parquet/atlas-full/
        graffold-ingest audit ./programs/my-prog/v1/ -o audit-report.md
    """
    from .audit import run_audit

    console.print(f"[cyan]Auditing:[/] {program_dir}")
    console.print(f"[dim]Graph: {graph}[/]")

    report = run_audit(program_dir=program_dir, parquet_root=graph)

    # Summary
    verdict_color = "green" if report.verdict == "CERTIFIED" else "red"
    console.print(f"\n[{verdict_color} bold]{report.verdict}[/] — {len(report.findings)} findings")

    for f in report.findings:
        icon = {"P0": "❌", "P1": "⚠️ ", "P2": "ℹ️ ", "P3": "💡"}.get(f.severity, " ")
        sev_color = {"P0": "red", "P1": "yellow", "P2": "blue", "P3": "dim"}.get(f.severity, "white")
        console.print(f"  {icon} [{sev_color}][{f.severity}][/] {f.domain}: {f.message[:90]}")

    md = report.to_markdown()
    if output:
        from pathlib import Path
        Path(output).write_text(md)
        console.print(f"\n[green]✓[/] Report: {output}")
    else:
        console.print("\n[dim]Use -o to save report as markdown[/]")


@cli.command()
@click.argument("disease")
@click.option("--graph", default="~/.graffold/parquet/atlas-full", help="Parquet graph directory")
@click.option("--output", "-o", default="", help="Output file (default: stdout)")
@click.option("--format", "-f", "fmt", type=click.Choice(["md", "json"]), default="md")
def query(disease: str, graph: str, output: str, fmt: str) -> None:
    """Query the knowledge graph for prior knowledge about a disease/topic.

    Generates a prior-knowledge document that Atlas can consume at startup.

    Examples:
        graffold-ingest query cryptosporidiosis
        graffold-ingest query "bovine mastitis" -o prior-knowledge.md
        graffold-ingest query cryptosporidiosis -f json
    """
    import json
    from pathlib import Path

    from .query import QueryEngine

    console.print(f"[cyan]Querying:[/] {disease}")
    console.print(f"[dim]Graph: {graph}[/]")

    engine = QueryEngine(graph)
    stats = engine.stats()
    console.print(f"  {stats['total_entities']} entities, {stats['total_relationships']} relationships")

    if fmt == "json":
        result = {
            "prior_knowledge": engine.prior_knowledge(disease),
            "kills": engine.cross_run_kills(disease),
            "evidence_gaps": engine.evidence_gaps(),
            "predictions": engine.novel_predictions(),
            "stats": stats,
        }
        text = json.dumps(result, indent=2, default=str)
    else:
        text = engine.prior_knowledge(disease)

    if output:
        Path(output).write_text(text)
        console.print(f"[green]✓[/] Written: {output} ({len(text):,} chars)")
    else:
        console.print()
        console.print(text[:3000])
        if len(text) > 3000:
            console.print(f"\n[dim]... ({len(text):,} chars total, use -o to save)[/]")


@cli.command()
@click.argument("target_name")
@click.option("--graph", default="~/.graffold/parquet/atlas-full", help="Parquet graph directory")
def trajectory(target_name: str, graph: str) -> None:
    """Show the full history of a target across all runs.

    Examples:
        graffold-ingest trajectory CpTrxR
        graffold-ingest trajectory "BCL2A1"
    """
    from .query import QueryEngine

    engine = QueryEngine(graph)
    traj = engine.target_trajectory(target_name)

    console.print(f"[cyan]Target:[/] {target_name}")
    console.print(f"  Mentions: {traj['mention_count']}")
    console.print(f"  Programs: {', '.join(traj['programs'])}")

    if traj['status_history']:
        console.print("\n  [bold]Status history:[/]")
        for s in traj['status_history'][:10]:
            console.print(f"    [{s['program'][:20]}] {s['status'][:70]}")

    if traj['relationships']:
        console.print(f"\n  [bold]Relationships:[/] {len(traj['relationships'])}")
        for r in traj['relationships'][:8]:
            console.print(f"    {r.get('type','?'):20s} → {r.get('target_id','?')[:30]}")


@cli.command()
@click.argument("program_dir", required=False)
@click.option("--llm/--no-llm", default=True, help="Run LLM extraction (default: yes)")
@click.option("--resolve/--no-resolve", default=False, help="Resolve entities via UniProt/MONDO/PubChem")
@click.option("--chunks", "-n", default=0, type=int, help="Max chunks to process (0=all)")
def ingest(program_dir: str | None, llm: bool, resolve: bool, chunks: int) -> None:
    """Ingest an Atlas program directory into the knowledge graph.

    Reads phase-*.md files, extracts entities via regex + LLM, writes to Parquet.
    Uses config from ~/.graffold/config.toml (run `graffold-ingest init` first).

    If no directory is given, uses atlas_programs_dir from config.

    Examples:
        graffold-ingest ingest ~/atlas/programs/crypto-v11/v1/
        graffold-ingest ingest --no-llm
        graffold-ingest ingest -n 20
    """
    import re
    import time
    from pathlib import Path

    from .connectors.agteria import AgteriaConnector
    from .global_config import Config
    from .pipeline.chunk import chunk_documents
    from .pipeline.publish_parquet import publish_to_parquet
    from .pipeline.resolve import resolve_entities

    cfg = Config.load()

    # Resolve program dir
    if program_dir:
        path = Path(program_dir).expanduser().resolve()
    elif cfg.atlas_programs_dir:
        path = Path(cfg.atlas_programs_dir)
        console.print(f"[dim]Using atlas dir from config: {path}[/]")
    else:
        console.print("[red]No directory specified and atlas_programs_dir not configured.[/]")
        console.print("Run: graffold-ingest ingest <path> or graffold-ingest init")
        return

    if not path.is_dir():
        console.print(f"[red]Not a directory:[/] {path}")
        return

    console.print(f"[cyan]Ingesting:[/] {path}")
    t_start = time.time()

    async def _run():
        connector = AgteriaConnector()

        # Fetch
        docs = await connector.fetch(path=str(path))
        if not docs:
            console.print("  No phase files found.")
            return
        total_chars = sum(len(d.content) for d in docs)
        console.print(f"  {len(docs)} files, {total_chars:,} chars")

        # Regex extraction
        regex_results = await connector.extract_direct(path=str(path))
        rn = sum(len(r.nodes) for r in regex_results)
        re_ = sum(len(r.edges) for r in regex_results)
        console.print(f"  Regex: {rn} entities, {re_} rels")

        all_results = list(regex_results)

        # LLM extraction
        if llm and cfg.is_configured:
            from .connectors.base import ExtractionResult
            from .pipeline.extract import EXTRACTION_PROMPT, _call_anthropic

            all_chunks = chunk_documents(docs, chunk_size=4000)
            n = chunks if chunks > 0 else len(all_chunks)
            selected = all_chunks[:n]
            console.print(f"  LLM: {n}/{len(all_chunks)} chunks via {cfg.llm_model} ({cfg.max_concurrent} concurrent)...")

            sem = asyncio.Semaphore(cfg.max_concurrent)
            successes = 0

            # Ensure API key is in env for the extraction call
            import os
            if cfg.anthropic_api_key and not os.getenv("ANTHROPIC_API_KEY"):
                os.environ["ANTHROPIC_API_KEY"] = cfg.anthropic_api_key

            async def _extract(text, chunk_id):
                nonlocal successes
                async with sem:
                    prompt = EXTRACTION_PROMPT.format(text=text[:8000])
                    try:
                        raw = await _call_anthropic(prompt, cfg.llm_model)
                        cleaned = re.sub(r'```(?:json)?\s*\n?', '', raw).strip()
                        if cleaned.endswith('```'):
                            cleaned = cleaned[:-3].strip()
                        import json
                        data = json.loads(cleaned)
                        successes += 1
                        return ExtractionResult(
                            nodes=data.get('nodes', []),
                            edges=data.get('edges', []),
                            source_doc_id=chunk_id,
                        )
                    except Exception:
                        return ExtractionResult(source_doc_id=chunk_id)

            tasks = [_extract(c.content, c.id) for c in selected]
            llm_results = await asyncio.gather(*tasks)
            llm_results = [r for r in llm_results if r.nodes]
            ln = sum(len(r.nodes) for r in llm_results)
            le = sum(len(r.edges) for r in llm_results)
            console.print(f"  LLM: {ln} entities, {le} rels ({successes}/{n} chunks ok)")
            all_results.extend(llm_results)
        elif llm and not cfg.is_configured:
            console.print("  [yellow]LLM skipped (no API key)[/]")

        # Resolve
        if resolve:
            from .resolvers.composite import CompositeResolver
            from .resolvers.mondo import MONDOResolver
            from .resolvers.pubchem import PubChemResolver
            from .resolvers.uniprot import UniProtResolver

            resolver = CompositeResolver([UniProtResolver(), MONDOResolver(), PubChemResolver()])
            resolved_count = 0
            for r in all_results:
                for node in r.nodes:
                    res = await resolver.resolve(node.get("name", ""), (node.get("label") or node.get("type") or "").lower())
                    if res:
                        node["canonical_id"] = res.canonical_id
                        node["canonical_name"] = res.canonical_name
                        resolved_count += 1
            console.print(f"  Resolved: {resolved_count} entities")

        # Dedup + publish
        deduped = resolve_entities(all_results)
        fn = sum(len(r.nodes) for r in deduped)
        fe = sum(len(r.edges) for r in deduped)

        # Output dir: use program name as subdir
        program_name = path.name
        if program_name in ('v1', 'v2', 'v3'):
            program_name = f"{path.parent.name}-{program_name}"
        out_dir = cfg.parquet_dir / program_name
        out_dir.mkdir(parents=True, exist_ok=True)

        await publish_to_parquet(deduped, output_dir=str(out_dir))
        elapsed = time.time() - t_start
        console.print(f"\n  [green]\u2713[/] {fn} entities, {fe} rels \u2192 {out_dir}/")
        console.print(f"  [dim]{elapsed:.1f}s[/]")

    asyncio.run(_run())


@cli.command()
@click.option("--api-key", prompt="Anthropic API key", hide_input=True, default="", help="Claude API key (sk-ant-...)")
@click.option("--atlas-dir", prompt="Atlas programs/ directory", default="", help="Path to Atlas programs/ directory")
def init(api_key: str, atlas_dir: str) -> None:
    """Set up graffold-ingest for first use.

    Creates ~/.graffold/, writes config.toml, validates API key.
    """
    from .global_config import Config

    cfg = Config()
    cfg.config_dir.mkdir(parents=True, exist_ok=True)
    cfg.parquet_dir.mkdir(parents=True, exist_ok=True)

    if api_key:
        cfg.anthropic_api_key = api_key
    if atlas_dir:
        from pathlib import Path
        atlas_path = Path(atlas_dir).expanduser().resolve()
        cfg.atlas_programs_dir = str(atlas_path)
        if not atlas_path.is_dir():
            console.print(f"[yellow]Warning:[/] {atlas_dir} not found (will use when created)")

    cfg.save()
    console.print(f"[green]\u2713[/] Config: {cfg.config_dir / 'config.toml'}")
    console.print(f"[green]\u2713[/] Store: {cfg.parquet_dir}/")

    if cfg.anthropic_api_key:
        import asyncio

        import httpx

        async def _check():
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    "https://api.anthropic.com/v1/models",
                    headers={"x-api-key": cfg.anthropic_api_key, "anthropic-version": "2023-06-01"},
                )
                return resp.status_code == 200

        if asyncio.run(_check()):
            console.print("[green]\u2713[/] API key valid")
        else:
            console.print("[red]\u2717[/] API key invalid")
    else:
        console.print("[yellow]![/] No API key \u2014 set ANTHROPIC_API_KEY or re-run init")

    console.print("\n[bold]Ready.[/] Try:")
    console.print("  graffold-ingest status")
    console.print("  graffold-ingest watch <atlas-programs-dir>")


@cli.command()
def status() -> None:
    """Show graph status and configuration."""
    from pathlib import Path

    from .global_config import Config

    cfg = Config.load()

    console.print("[bold]graffold-ingest[/] status\n")

    # Config
    console.print("[cyan]Config[/]")
    if (cfg.config_dir / "config.toml").exists():
        console.print(f"  File: {cfg.config_dir / 'config.toml'}")
    else:
        console.print("  [yellow]Not initialized[/] \u2014 run `graffold-ingest init`")
        return
    console.print(f"  LLM: {cfg.llm_service} / {cfg.llm_model}")
    console.print(f"  Key: {'[green]set[/]' if cfg.is_configured else '[red]not set[/]'}")
    console.print(f"  Atlas: {cfg.atlas_programs_dir or '[dim]not configured[/]'}")
    console.print()

    # Graph
    console.print("[cyan]Graph[/]")
    parquet_dir = cfg.parquet_dir
    if not parquet_dir.exists():
        console.print(f"  {parquet_dir}/ [yellow](empty)[/]")
        return

    total_entities = 0
    total_rels = 0
    programs = []

    for item in sorted(parquet_dir.iterdir()):
        if item.is_dir() and (item / "entities.parquet").exists():
            import pyarrow.parquet as pq
            ent = pq.read_metadata(item / "entities.parquet").num_rows
            rel = pq.read_metadata(item / "relationships.parquet").num_rows if (item / "relationships.parquet").exists() else 0
            total_entities += ent
            total_rels += rel
            programs.append((item.name, ent, rel))
        elif item.name == "entities.parquet":
            import pyarrow.parquet as pq
            total_entities = pq.read_metadata(item).num_rows
            if (parquet_dir / "relationships.parquet").exists():
                total_rels = pq.read_metadata(parquet_dir / "relationships.parquet").num_rows
            programs.append(("(root)", total_entities, total_rels))

    console.print(f"  Store: {parquet_dir}/")
    console.print(f"  [bold]{total_entities:,}[/] entities, [bold]{total_rels:,}[/] relationships")
    if programs:
        console.print(f"  Programs: {len(programs)}")
        for name, ents, rels in programs[:5]:
            console.print(f"    {name[:30]:30s} {ents:>6,} ent  {rels:>6,} rel")
        if len(programs) > 5:
            console.print(f"    ... +{len(programs) - 5} more")
    console.print()

    # Atlas
    if cfg.atlas_programs_dir:
        atlas_path = Path(cfg.atlas_programs_dir)
        if atlas_path.is_dir():
            phase_count = len(list(atlas_path.rglob("phase-*.md")))
            console.print("[cyan]Atlas[/]")
            console.print(f"  Dir: {atlas_path}")
            console.print(f"  Phase files: {phase_count}")


@cli.command()
@click.argument("question")
@click.option("--graph", default="", help="Parquet graph dir (default: from config)")
def ask(question: str, graph: str) -> None:
    """Ask a natural language question about the knowledge graph.

    Uses the accumulated graph + Claude to answer research questions.

    Examples:
        graffold-ingest ask "What targets exist for cryptosporidiosis?"
        graffold-ingest ask "Why was Auranofin killed?"
        graffold-ingest ask "What evidence supports CpTrxR as a target?"
    """
    import os
    from pathlib import Path

    import pyarrow.parquet as pq

    from .global_config import Config

    cfg = Config.load()
    if not cfg.is_configured:
        console.print("[red]No API key configured.[/] Run `graffold-ingest init`")
        return

    # Load graph
    graph_dir = Path(graph).expanduser() if graph else cfg.parquet_dir / "atlas-full"
    if not (graph_dir / "entities.parquet").exists():
        # Try parquet_dir root
        if (cfg.parquet_dir / "entities.parquet").exists():
            graph_dir = cfg.parquet_dir
        else:
            console.print(f"[red]No graph found at {graph_dir}[/]")
            console.print("Run `graffold-ingest ingest` first.")
            return

    entities = pq.read_table(graph_dir / "entities.parquet").to_pylist()
    rels = pq.read_table(graph_dir / "relationships.parquet").to_pylist() if (graph_dir / "relationships.parquet").exists() else []

    console.print(f"[dim]Graph: {len(entities):,} entities, {len(rels):,} rels[/]")
    console.print("[dim]Searching...[/]\n")

    # Build context: find entities matching the question terms
    q_lower = question.lower()
    terms = [w for w in q_lower.split() if len(w) > 3]

    # Score entities by term match
    scored = []
    for e in entities:
        name = (e.get("name") or "").lower()
        desc = (e.get("description") or "").lower()
        score = sum(1 for t in terms if t in name or t in desc)
        if score > 0:
            scored.append((score, e))
    scored.sort(key=lambda x: -x[0])
    top_entities = [e for _, e in scored[:30]]

    # Find relationships involving top entities
    top_ids = {e["id"] for e in top_entities}
    id_to_name = {e["id"]: e.get("name", "") for e in entities}
    relevant_rels = []
    for r in rels:
        if r.get("source_id") in top_ids or r.get("target_id") in top_ids:
            relevant_rels.append(r)
            if len(relevant_rels) >= 50:
                break

    # Build context for LLM
    context_lines = ["Knowledge graph context:"]
    context_lines.append(f"\nEntities ({len(top_entities)} most relevant):")
    for e in top_entities[:20]:
        t = e.get("type", "?")
        desc = e.get("description", "")[:80]
        context_lines.append(f"  [{t}] {e.get('name', '?')}: {desc}")

    context_lines.append(f"\nRelationships ({len(relevant_rels)} relevant):")
    for r in relevant_rels[:30]:
        src = id_to_name.get(r.get("source_id", ""), "?")[:25]
        tgt = id_to_name.get(r.get("target_id", ""), "?")[:25]
        context_lines.append(f"  {src} --[{r.get('type', '?')}]--> {tgt}")

    context = "\n".join(context_lines)

    prompt = f"""You are a drug discovery research assistant with access to a knowledge graph.
Answer the following question using ONLY the provided graph context. Be specific, cite entity names.
If the graph doesn't contain enough information, say so.

Graph context:
{context}

Question: {question}

Answer:"""

    # Call LLM
    if not os.getenv("ANTHROPIC_API_KEY"):
        os.environ["ANTHROPIC_API_KEY"] = cfg.anthropic_api_key

    from .pipeline.extract import _call_anthropic

    answer = asyncio.run(_call_anthropic(prompt, cfg.llm_model))
    console.print(answer)
    console.print(f"\n[dim]({len(top_entities)} entities, {len(relevant_rels)} rels used as context)[/]")


@cli.command()
@click.argument("disease")
@click.option("--program-dir", "-d", default="", help="Write prior-knowledge.md into this directory")
@click.option("--output", "-o", default="", help="Output file path (default: prior-knowledge.md in program-dir or stdout)")
def context(disease: str, program_dir: str, output: str) -> None:
    """Generate startup context for an Atlas run from the knowledge graph.

    Produces a prior-knowledge.md that Atlas can consume, containing
    explored targets, kill decisions, evidence, and mechanism clusters.

    Examples:
        graffold-ingest context cryptosporidiosis
        graffold-ingest context cryptosporidiosis -d ~/atlas/programs/crypto-v12/v1/
        graffold-ingest context "bovine mastitis" -o context.md
    """
    from pathlib import Path

    import pyarrow.parquet as pq

    from .global_config import Config

    cfg = Config.load()
    graph_dir = cfg.parquet_dir / "atlas-full"

    # Find graph
    if not (graph_dir / "entities.parquet").exists():
        # Try root
        if (cfg.parquet_dir / "entities.parquet").exists():
            graph_dir = cfg.parquet_dir
        else:
            console.print("[red]No graph found.[/] Run `graffold-ingest ingest` first.")
            return

    entities = pq.read_table(graph_dir / "entities.parquet").to_pylist()
    rels = pq.read_table(graph_dir / "relationships.parquet").to_pylist() if (graph_dir / "relationships.parquet").exists() else []

    # Get latest version of each entity (append-only store)
    seen: dict[str, dict] = {}
    for e in entities:
        eid = e.get("id", "")
        if eid not in seen or e.get("ingested_at", 0) > seen[eid].get("ingested_at", 0):
            seen[eid] = e
    entities = list(seen.values())

    # Categorize
    targets = [e for e in entities if "target" in (e.get("type") or "").lower()]
    [e for e in entities if "compound" in (e.get("type") or "").lower()]
    mechanisms = [e for e in entities if "mechanism" in (e.get("type") or "").lower()]
    evidence = [e for e in entities if "evidence" in (e.get("type") or "").lower()]
    [e for e in entities if "decision" in (e.get("type") or "").lower()]

    id_to_name = {e["id"]: e.get("name", "") for e in entities}
    kills = [r for r in rels if r.get("type") == "KILLED_BECAUSE"]
    inhibits = [r for r in rels if r.get("type") == "INHIBITS"]
    [r for r in rels if r.get("type") == "VALIDATED_BY"]

    # Build document
    lines = [
        f"# Prior Knowledge: {disease.title()}",
        "",
        "> Generated by graffold-ingest from accumulated Atlas runs.",
        f"> **{len(entities):,} entities, {len(rels):,} relationships** in the graph.",
        "> Feed this file to Atlas at startup for cross-run memory.",
        "",
        "---",
        "",
        f"## Target Landscape ({len(targets)} explored)",
        "",
        "| Target | Description |",
        "|--------|-------------|",
    ]
    seen_names: set[str] = set()
    for t in sorted(targets, key=lambda x: x.get("name", "")):
        name = t.get("name", "")
        if name.lower() in seen_names or len(name) < 4:
            continue
        seen_names.add(name.lower())
        desc = (t.get("description") or "")[:60].replace("|", "/")
        lines.append(f"| {name[:45]} | {desc} |")
        if len(seen_names) >= 50:
            break

    lines += ["", f"*({len(targets)} total targets in graph)*", ""]

    # Kills
    if kills:
        lines += [
            f"## Kill Decisions ({len(kills)} total)",
            "",
            "**Do NOT re-propose without resurrection conditions:**",
            "",
        ]
        kill_seen: set[str] = set()
        for r in kills:
            src = id_to_name.get(r.get("source_id", ""), "?")
            if src.lower() in kill_seen:
                continue
            kill_seen.add(src.lower())
            tgt = id_to_name.get(r.get("target_id", ""), "")
            desc = (r.get("description") or "")[:80]
            lines.append(f"- **{src}**: {desc or tgt}")
            if len(kill_seen) >= 25:
                break
        lines.append("")

    # Compounds
    if inhibits:
        lines += ["## Compound-Target Inhibitions", ""]
        inh_seen: set[str] = set()
        for r in inhibits[:20]:
            src = id_to_name.get(r.get("source_id", ""), "?")
            tgt = id_to_name.get(r.get("target_id", ""), "?")
            key = f"{src}->{tgt}"
            if key not in inh_seen:
                inh_seen.add(key)
                lines.append(f"- **{src}** inhibits **{tgt}**")
        lines.append("")

    # Mechanisms
    if mechanisms:
        lines += ["## Mechanisms & Pathways", ""]
        for m in mechanisms[:20]:
            lines.append(f"- **{m.get('name', '?')}**: {(m.get('description') or '')[:60]}")
        lines.append("")

    # Evidence
    if evidence:
        lines += [f"## Evidence Base ({len(evidence)} citations)", ""]
        for ev in evidence[:15]:
            lines.append(f"- {ev.get('name', '?')}: {(ev.get('description') or '')[:60]}")
        lines.append("")

    lines += [
        "---",
        "",
        "*This document is append-only source-of-truth. Every assertion is timestamped.",
        "Query with: `graffold-ingest ask \"<question>\"`*",
    ]

    text = "\n".join(lines)

    # Output
    if output:
        Path(output).write_text(text)
        console.print(f"[green]\u2713[/] {output} ({len(text):,} chars)")
    elif program_dir:
        out_path = Path(program_dir).expanduser() / "prior-knowledge.md"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text)
        console.print(f"[green]\u2713[/] {out_path} ({len(text):,} chars)")
    else:
        console.print(text)


@cli.command()
@click.argument("graph_dir")
@click.option("--output", "-o", default="", help="Output dir (default: <graph_dir>-harmonized)")
@click.option("--embeddings/--no-embeddings", default=True, help="Use embedding merge pass")
@click.option("--threshold", default=0.90, type=float, help="Embedding cosine threshold")
def harmonize(graph_dir: str, output: str, embeddings: bool, threshold: float) -> None:
    """Collapse fragmented entities into canonical nodes.

    Global pass over an assembled graph: alias rules (deterministic) +
    optional within-type embedding merge. Fixes literature fragmentation
    where one entity (F18, heat-labile toxin) appears as dozens of nodes.

    Examples:
        graffold-ingest harmonize ~/.graffold/parquet/etec-pigs
        graffold-ingest harmonize ./graph -o ./graph-clean --no-embeddings
    """
    import shutil
    from pathlib import Path

    from .connectors.base import ExtractionResult
    from .pipeline.harmonize import harmonize_graph
    from .pipeline.publish_parquet import publish_to_parquet, read_parquet_graph

    src = Path(graph_dir).expanduser()
    dst = Path(output).expanduser() if output else src.parent / f"{src.name}-harmonized"

    console.print(f"[cyan]Harmonizing:[/] {src}")
    n, e = read_parquet_graph(src, latest=True)
    console.print(f"  Loaded {len(n)} entities, {len(e)} relationships")

    fn, fe, rep = harmonize_graph(n, e, use_embeddings=embeddings, embed_threshold=threshold)

    console.print(f"  Alias merges:     {rep.alias_merges}")
    console.print(f"  Embedding merges: {rep.embedding_merges}")
    console.print(f"  [green]OK[/] {rep.entities_before} -> {rep.entities_after} entities, "
                  f"{rep.edges_before} -> {rep.edges_after} relationships")

    if rep.merge_examples:
        console.print("\n  [dim]Sample merges:[/]")
        for orig, canon in rep.merge_examples[:8]:
            console.print(f"    {orig[:38]:38s} -> {canon[:32]}")

    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)
    result = ExtractionResult(nodes=fn, edges=fe, source_doc_id=f"{src.name}:harmonized")
    asyncio.run(publish_to_parquet([result], output_dir=dst, run_id="harmonized"))
    console.print(f"\n  [green]OK[/] Written to {dst}")


@cli.command("ingest-corpus")
@click.option("--queries", required=True, help="Path to file with one search query per line")
@click.option("--source", type=click.Choice(["pubmed", "europepmc"]), default="pubmed")
@click.option("--service", default="bedrock-llama", help="LLM service for extraction")
@click.option("--output", "-o", required=True, help="Parquet output directory")
@click.option("--per-query", default=40, type=int, help="Max papers per query")
@click.option("--paper-cap", default=1000, type=int, help="Total unique paper cap")
@click.option("--full-text/--abstract", default=True, help="Europe PMC: fetch OA full text")
@click.option("--relevance", default="", help="Regex; papers must match to be kept")
@click.option("--harmonize/--no-harmonize", default=True, help="Run harmonization after ingest")
@click.option("--concurrent", default=5, type=int, help="Concurrent LLM extractions")
def ingest_corpus(queries: str, source: str, service: str, output: str, per_query: int,
                  paper_cap: int, full_text: bool, relevance: str, harmonize: bool,
                  concurrent: int) -> None:
    """Ingest a whole literature corpus from a query list into one graph.

    Runs fetch -> extract -> fuzzy-resolve -> (harmonize) -> publish over
    every query in the file, deduplicating papers across queries. This is
    the product command behind the ETEC case study.

    Checkpointed: re-running skips already-processed papers.

    Example:
        graffold-ingest ingest-corpus --queries etec-queries.txt \
          --source pubmed --service bedrock-llama \
          -o ~/.graffold/parquet/etec --paper-cap 1000
    """
    import json
    import re
    import time
    from pathlib import Path

    from .connectors import CONNECTORS
    from .connectors.base import Document, ExtractionResult
    from .pipeline.chunk import chunk_documents
    from .pipeline.extract import extract_entities_parallel
    from .pipeline.publish_parquet import publish_to_parquet, read_parquet_graph
    from .resolvers.local import EntityResolver

    qpath = Path(queries).expanduser()
    if not qpath.exists():
        console.print(f"[red]Query file not found:[/] {qpath}")
        return
    query_list = [q.strip() for q in qpath.read_text().splitlines() if q.strip() and not q.startswith("#")]
    out_dir = Path(output).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = out_dir / ".corpus_checkpoint.json"
    processed = set()
    if checkpoint.exists():
        try:
            processed = set(json.loads(checkpoint.read_text()).get("processed", []))
        except Exception:
            pass

    rel_re = re.compile(relevance, re.IGNORECASE) if relevance else None
    console.print(f"[cyan]Corpus ingest:[/] {len(query_list)} queries -> {source} -> {out_dir}")
    console.print(f"  Cap: {paper_cap} papers | already processed: {len(processed)}")

    async def _run():
        connector = CONNECTORS[source]()
        # Gather unique papers across all queries
        all_docs: dict[str, Document] = {}
        for q in query_list:
            if len(all_docs) >= paper_cap:
                break
            kwargs = {"query": q, "limit": per_query}
            if source == "europepmc":
                kwargs["full_text"] = full_text
            docs = await connector.fetch(**kwargs)
            for d in docs:
                key = d.metadata.get("pmid") or d.id
                if not key or key in processed or key in all_docs:
                    continue
                if rel_re and not rel_re.search(d.content[:3000]):
                    continue
                all_docs[key] = d
                if len(all_docs) >= paper_cap:
                    break
            console.print(f"  {q[:44]:44s} (unique: {len(all_docs)})")

        docs = list(all_docs.values())[:paper_cap]
        ft = sum(1 for d in docs if len(d.content) > 5000)
        console.print(f"\n  {len(docs)} papers ({ft} full-text). Extracting via {service}...")

        chunks = chunk_documents(docs, chunk_size=4000)
        resolver = EntityResolver(enable_fuzzy=True)
        t0 = time.time()
        total_e = total_r = 0
        BATCH = 25
        for i in range(0, len(chunks), BATCH):
            batch = chunks[i:i + BATCH]
            results = await extract_entities_parallel(batch, llm_service=service, max_concurrent=concurrent)
            results = [r for r in results if r and r.nodes]
            if results:
                nn = [n for r in results for n in r.nodes]
                ee = [e for r in results for e in r.edges]
                mn, me = resolver.resolve(nn, ee)
                c = await publish_to_parquet(
                    [ExtractionResult(nodes=mn, edges=me, source_doc_id=f"{source}:batch-{i // BATCH}")],
                    output_dir=out_dir, run_id=f"corpus-{i // BATCH}")
                total_e += c["entities_written"]
                total_r += c["relationships_written"]
            for c_ in batch:
                processed.add(c_.id.split("_chunk")[0].replace("pmid:", ""))
            checkpoint.write_text(json.dumps({"processed": sorted(processed)}))
            console.print(f"    batch {i // BATCH + 1}/{(len(chunks) + BATCH - 1) // BATCH}: "
                          f"+{sum(len(r.nodes) for r in results)} ent [{time.time() - t0:.0f}s]")

        console.print(f"\n  [green]OK[/] {total_e} entities, {total_r} relationships in {(time.time()-t0)/60:.1f} min")

        if harmonize:
            from .pipeline.harmonize import harmonize_graph
            console.print("\n  [cyan]Harmonizing...[/]")
            n, e = read_parquet_graph(out_dir, latest=True)
            fn, fe, rep = harmonize_graph(n, e, use_embeddings=True)
            console.print(f"    {rep.entities_before} -> {rep.entities_after} entities "
                          f"({rep.alias_merges} alias, {rep.embedding_merges} embedding)")
            import shutil
            hdir = out_dir.parent / f"{out_dir.name}-harmonized"
            if hdir.exists():
                shutil.rmtree(hdir)
            hdir.mkdir(parents=True)
            await publish_to_parquet(
                [ExtractionResult(nodes=fn, edges=fe, source_doc_id="harmonized")],
                output_dir=hdir, run_id="harmonized")
            console.print(f"    [green]OK[/] Harmonized graph -> {hdir}")

    asyncio.run(_run())


def main() -> None:
    cli()


if __name__ == "__main__":
    main()