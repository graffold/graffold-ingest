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


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
