"""graffold-ingest CLI."""

from __future__ import annotations

import asyncio

import click
from rich.console import Console

console = Console()


@click.group(invoke_without_command=True)
@click.version_option("0.1.0", prog_name="graffold-ingest")
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
@click.option("--source", type=click.Choice(["web", "pdf", "api", "csv", "database"]), required=True)
@click.option("--url", default="")
@click.option("--path", default="")
@click.option("--service", default="bedrock")
@click.option("--database-uri", default="bolt://localhost:7687")
def pipeline(source: str, url: str, path: str, service: str, database_uri: str) -> None:
    """Run the full ingestion pipeline."""
    from .connectors import CONNECTORS
    from .pipeline import chunk_documents, extract_entities, publish_to_graph, resolve_entities

    console.print(f"[cyan]Pipeline:[/] {source} → extract → publish")

    async def _run():
        connector = CONNECTORS[source]()
        kwargs = {}
        if url:
            kwargs["url"] = url
        if path:
            kwargs["path"] = path

        docs = await connector.fetch(**kwargs)
        console.print(f"  Fetched {len(docs)} documents")

        chunks = chunk_documents(docs)
        console.print(f"  Chunked into {len(chunks)} pieces")

        results = await extract_entities(chunks, llm_service=service)
        total_nodes = sum(len(r.nodes) for r in results)
        total_edges = sum(len(r.edges) for r in results)
        console.print(f"  Extracted {total_nodes} nodes, {total_edges} edges")

        results = resolve_entities(results)
        console.print("  Resolved duplicates")

        counts = await publish_to_graph(results, database_uri=database_uri)
        console.print(f"  [green]✓[/] Published: {counts}")

    asyncio.run(_run())


@cli.command()
def tui() -> None:
    """Launch the interactive terminal UI."""
    from .tui import main as tui_main

    tui_main()


@cli.command()
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8000, type=int)
def serve(host: str, port: int) -> None:
    """Run the ingest API server."""
    import uvicorn

    uvicorn.run("graffold_ingest.api:app", host=host, port=port)


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


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
