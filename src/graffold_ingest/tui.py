"""graffold-ingest interactive TUI.

Arrow-key navigated terminal interface for the full ingestion pipeline.
"""

from __future__ import annotations

import os
import sys


def _clear():
    os.system("clear" if os.name != "nt" else "cls")


def _color(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m"


def _cyan(t: str) -> str: return _color(t, "36")
def _green(t: str) -> str: return _color(t, "32")
def _yellow(t: str) -> str: return _color(t, "33")
def _red(t: str) -> str: return _color(t, "31")
def _dim(t: str) -> str: return _color(t, "90")
def _bold(t: str) -> str: return _color(t, "1")


BANNER = r"""
  ▀▀▀▀▀▀  ▀▀▀▀▀▀   ▀▀▀▀▀  ▀▀▀▀▀▀▀ ▀▀▀▀▀▀▀  ▀▀▀▀▀▀  ▀▀      ▀▀▀▀▀▀
 ▀▀       ▀▀   ▀▀ ▀▀   ▀▀ ▀▀      ▀▀      ▀▀    ▀▀ ▀▀      ▀▀   ▀▀
 ▀▀   ▀▀▀ ▀▀▀▀▀▀  ▀▀▀▀▀▀▀ ▀▀▀▀▀   ▀▀▀▀▀   ▀▀    ▀▀ ▀▀      ▀▀   ▀▀
 ▀▀    ▀▀ ▀▀   ▀▀ ▀▀   ▀▀ ▀▀      ▀▀      ▀▀    ▀▀ ▀▀      ▀▀   ▀▀
  ▀▀▀▀▀▀  ▀▀   ▀▀ ▀▀   ▀▀ ▀▀      ▀▀       ▀▀▀▀▀▀  ▀▀▀▀▀▀▀ ▀▀▀▀▀▀
                         ─── ingest ───
"""


def _read_key() -> str:
    import termios
    import tty

    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
        if ch == "\x1b":
            ch2 = sys.stdin.read(1)
            if ch2 == "[":
                ch3 = sys.stdin.read(1)
                if ch3 == "A":
                    return "up"
                if ch3 == "B":
                    return "down"
            return "esc"
        if ch in ("\r", "\n"):
            return "enter"
        if ch == "\x03":
            raise KeyboardInterrupt
        return ch
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


def _menu(title: str, options: list[str], descriptions: list[str] | None = None) -> int:
    selected = 0
    while True:
        _clear()
        print(_cyan(BANNER))
        print(f"  {_bold(title)}\n")

        for i, opt in enumerate(options):
            prefix = _green("  ▸ ") if i == selected else "    "
            label = _bold(opt) if i == selected else opt
            desc = ""
            if descriptions and i < len(descriptions):
                desc = _dim(f"  {descriptions[i]}")
            print(f"{prefix}{label}{desc}")

        print(f"\n  {_dim('↑↓ navigate  Enter select  q quit')}")

        key = _read_key()
        if key == "up":
            selected = (selected - 1) % len(options)
        elif key == "down":
            selected = (selected + 1) % len(options)
        elif key == "enter":
            return selected
        elif key in ("q", "esc"):
            return -1


def _pause():
    print(f"\n  {_dim('Press any key to continue...')}")
    _read_key()


def _input_prompt(label: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    try:
        val = input(f"  {_cyan(label)}{suffix}: ").strip()
        return val or default
    except (EOFError, KeyboardInterrupt):
        return default


# ─── Fetch ─────────────────────────────────────────────────────────────────


def _fetch_menu():
    while True:
        choice = _menu(
            "① Fetch Data — Acquire content from sources",
            [
                "Web Pages",
                "PDF Documents",
                "REST API",
                "CSV / Excel / Parquet",
                "Database (SQL)",
                "Smart Agent (auto-discover)",
                "← Back",
            ],
            [
                "scrape URLs or sitemaps",
                "extract text from local PDFs",
                "fetch JSON from API endpoints",
                "load tabular data files",
                "query a SQL database",
                "LLM-guided autonomous scraping",
                "",
            ],
        )
        if choice in (-1, 6):
            return
        if choice == 0:
            _fetch_web()
        elif choice == 1:
            _fetch_pdf()
        elif choice == 2:
            _fetch_api()
        elif choice == 3:
            _fetch_csv()
        elif choice == 4:
            _fetch_db()
        elif choice == 5:
            _fetch_agent()


def _fetch_web():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('🌐 Fetch Web Pages')}\n")
    url = _input_prompt("URL")
    if not url:
        return
    print(f"\n  Fetching {_yellow(url)}...")
    import asyncio
    from .connectors.web import WebConnector

    docs = asyncio.run(WebConnector().fetch(url=url))
    print(f"  {_green('✓')} Got {len(docs)} document(s), {sum(len(d.content) for d in docs)} chars")
    _pause()


def _fetch_pdf():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('📄 Fetch PDFs')}\n")
    path = _input_prompt("File or directory path")
    if not path:
        return
    import asyncio
    from .connectors.pdf import PdfConnector

    docs = asyncio.run(PdfConnector().fetch(path=path))
    print(f"  {_green('✓')} Extracted {len(docs)} document(s)")
    _pause()


def _fetch_api():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('🔌 Fetch from API')}\n")
    url = _input_prompt("API endpoint URL")
    if not url:
        return
    import asyncio
    from .connectors.api import ApiConnector

    docs = asyncio.run(ApiConnector().fetch(url=url))
    print(f"  {_green('✓')} Got {len(docs)} response(s)")
    _pause()


def _fetch_csv():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('📊 Load CSV/Excel')}\n")
    path = _input_prompt("File path")
    if not path:
        return
    import asyncio
    from .connectors.csv import CsvConnector

    docs = asyncio.run(CsvConnector().fetch(path=path))
    if docs:
        rows = docs[0].metadata.get("rows", "?")
        print(f"  {_green('✓')} Loaded {rows} rows")
    _pause()


def _fetch_db():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('🗄️  Query Database')}\n")
    conn = _input_prompt("Connection string (sqlite path or URI)")
    if not conn:
        return
    query = _input_prompt("SQL query", "SELECT * FROM sqlite_master LIMIT 10")
    import asyncio
    from .connectors.database import DatabaseConnector

    docs = asyncio.run(DatabaseConnector().fetch(connection_string=conn, query=query))
    if docs:
        rows = docs[0].metadata.get("row_count", "?")
        print(f"  {_green('✓')} Got {rows} rows")
    _pause()


def _fetch_agent():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('🤖 Smart Scraping Agent')}\n")
    url = _input_prompt("Starting URL")
    if not url:
        return
    service = _input_prompt("LLM service", "bedrock")
    print(f"\n  Agent crawling {_yellow(url)}...")
    import asyncio
    from .agent import scrape_agent

    try:
        result = asyncio.run(scrape_agent(url, llm_service=service))
        print(f"  {_green('✓')} {result}")
    except Exception as exc:
        print(f"  {_red('✗')} {exc}")
    _pause()


# ─── Extract ──────────────────────────────────────────────────────────────


def _extract_menu():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('🧠 Extract Entities')}\n")
    print(f"  {_dim('Runs LLM-powered schema-free extraction on fetched documents.')}\n")

    source = _input_prompt("Source (web/pdf/csv)", "web")
    target = _input_prompt("URL or path")
    if not target:
        return
    service = _input_prompt("LLM service", "bedrock")

    import asyncio
    from .connectors import CONNECTORS
    from .pipeline import chunk_documents, extract_entities

    async def _run():
        connector = CONNECTORS.get(source)
        if not connector:
            print(f"  {_red('✗')} Unknown source: {source}")
            return

        kwargs = {"url": target} if source in ("web", "api") else {"path": target}
        docs = await connector().fetch(**kwargs)
        print(f"  Fetched {len(docs)} docs")

        chunks = chunk_documents(docs)
        print(f"  Chunked into {len(chunks)} pieces")

        results = await extract_entities(chunks, llm_service=service)
        total_n = sum(len(r.nodes) for r in results)
        total_e = sum(len(r.edges) for r in results)
        print(f"\n  {_green('✓')} Extracted {total_n} entities, {total_e} relationships")

    try:
        asyncio.run(_run())
    except Exception as exc:
        print(f"  {_red('✗')} {exc}")
    _pause()


# ─── Build KG ─────────────────────────────────────────────────────────────


def _build_menu():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('🏗️  Build Knowledge Graph')}\n")
    print(f"  {_dim('Full pipeline: fetch → chunk → extract → resolve → publish')}\n")

    source = _input_prompt("Source type (web/pdf/csv/api)", "web")
    target = _input_prompt("URL or path")
    if not target:
        return
    service = _input_prompt("LLM service", "bedrock")
    db_uri = _input_prompt("Database URI", "bolt://localhost:7687")

    print(f"\n  {_cyan('Running full pipeline...')}\n")

    import asyncio
    from .connectors import CONNECTORS
    from .pipeline import chunk_documents, extract_entities, publish_to_graph, resolve_entities

    async def _run():
        connector = CONNECTORS.get(source)
        if not connector:
            print(f"  {_red('✗')} Unknown source: {source}")
            return

        kwargs = {"url": target} if source in ("web", "api") else {"path": target}
        docs = await connector().fetch(**kwargs)
        print(f"  ① Fetched {len(docs)} documents")

        chunks = chunk_documents(docs)
        print(f"  ② Chunked into {len(chunks)} pieces")

        results = await extract_entities(chunks, llm_service=service)
        total_n = sum(len(r.nodes) for r in results)
        total_e = sum(len(r.edges) for r in results)
        print(f"  ③ Extracted {total_n} entities, {total_e} relationships")

        results = resolve_entities(results)
        print("  ④ Resolved duplicates")

        counts = await publish_to_graph(results, database_uri=db_uri)
        print(f"  ⑤ {_green('✓')} Published: {counts}")

    try:
        asyncio.run(_run())
    except Exception as exc:
        print(f"  {_red('✗')} {exc}")
    _pause()


# ─── Schema Tools ─────────────────────────────────────────────────────────


def _schema_menu():
    while True:
        choice = _menu(
            "④ Schema Tools — Define your knowledge graph structure",
            [
                "Discover from sample data",
                "Discover from domain description",
                "Validate existing schema",
                "Refine with feedback",
                "View current schema",
                "← Back",
            ],
            [
                "LLM analyzes your data and proposes a schema",
                "describe your domain in plain English",
                "check schema.yaml for errors",
                "tell the LLM what to change",
                "show loaded entity/relationship types",
                "",
            ],
        )
        if choice in (-1, 5):
            return
        if choice == 0:
            _schema_discover_file()
        elif choice == 1:
            _schema_discover_domain()
        elif choice == 2:
            _schema_validate()
        elif choice == 3:
            _schema_refine()
        elif choice == 4:
            _schema_view()


def _schema_discover_file():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('🔍 Discover Schema from Data')}\n")

    path = _input_prompt("Sample file path (PDF, CSV, TXT)")
    if not path:
        return
    service = _input_prompt("LLM service", "bedrock")
    output = _input_prompt("Output file", "schema.yaml")

    from pathlib import Path

    content = Path(path).read_text(errors="ignore")[:8000]
    print(f"\n  Analyzing {len(content)} chars...")

    import asyncio
    from .pipeline.discover import discover_schema, save_schema

    try:
        yaml_content = asyncio.run(discover_schema(content=content, llm_service=service))
        save_schema(yaml_content, output)
        print(f"\n  {_green('✓')} Schema saved to {output}")
        print(f"\n{_dim(yaml_content[:400])}")
    except Exception as exc:
        print(f"  {_red('✗')} {exc}")
    _pause()


def _schema_discover_domain():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('🔍 Discover Schema from Domain')}\n")
    print(f"  {_dim('Describe your domain in plain English.')}")
    print(f"  {_dim('Example: \"cybersecurity threat intelligence - APTs, CVEs, malware families\"')}\n")

    domain = _input_prompt("Domain description")
    if not domain:
        return
    service = _input_prompt("LLM service", "bedrock")
    output = _input_prompt("Output file", "schema.yaml")

    print(f"\n  Generating schema for: {_yellow(domain)}...")

    import asyncio
    from .pipeline.discover import discover_schema, save_schema

    try:
        yaml_content = asyncio.run(discover_schema(domain=domain, llm_service=service))
        save_schema(yaml_content, output)
        print(f"\n  {_green('✓')} Schema saved to {output}")
        print(f"\n{_dim(yaml_content[:400])}")
    except Exception as exc:
        print(f"  {_red('✗')} {exc}")
    _pause()


def _schema_validate():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('✅ Validate Schema')}\n")

    path = _input_prompt("Schema file", "schema.yaml")
    from pathlib import Path

    from .pipeline.discover import validate_schema

    try:
        content = Path(path).read_text()
        issues = validate_schema(content)
        if issues:
            print(f"\n  {_red('✗')} {len(issues)} issue(s):")
            for issue in issues:
                print(f"    • {issue}")
        else:
            from .pipeline.schema import KGSchema

            s = KGSchema.load(path)
            print(f"\n  {_green('✓')} Valid schema")
            print(f"    {len(s.entities)} entity types, {len(s.relationships)} relationship types")
    except FileNotFoundError:
        print(f"  {_red('✗')} File not found: {path}")
    _pause()


def _schema_refine():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('✏️  Refine Schema')}\n")

    path = _input_prompt("Schema file", "schema.yaml")
    from pathlib import Path

    try:
        current = Path(path).read_text()
    except FileNotFoundError:
        print(f"  {_red('✗')} File not found: {path}")
        _pause()
        return

    print(f"  {_dim('Current schema loaded.')}")
    feedback = _input_prompt("What should change?")
    if not feedback:
        return
    service = _input_prompt("LLM service", "bedrock")

    import asyncio
    from .pipeline.discover import refine_schema, save_schema

    try:
        updated = asyncio.run(refine_schema(current, feedback, llm_service=service))
        save_schema(updated, path)
        print(f"\n  {_green('✓')} Updated {path}")
    except Exception as exc:
        print(f"  {_red('✗')} {exc}")
    _pause()


def _schema_view():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('📋 Current Schema')}\n")

    path = _input_prompt("Schema file", "schema.yaml")
    try:
        from .pipeline.schema import KGSchema

        s = KGSchema.load(path)
        print(f"  {_bold('Entities:')}")
        for e in s.entities:
            examples = ", ".join(e.examples[:3])
            print(f"    • {_cyan(e.name)}: {e.description}")
            print(f"      {_dim(f'e.g. {examples}')}")
        print(f"\n  {_bold('Relationships:')}")
        for r in s.relationships:
            src = " | ".join(r.source)
            tgt = " | ".join(r.target)
            print(f"    • {_cyan(r.type)}: {src} → {tgt}")
        print(f"\n  {_bold('Rules:')}")
        for rule in s.extraction_rules[:5]:
            print(f"    • {_dim(rule)}")
    except FileNotFoundError:
        print(f"  {_dim('No schema.yaml found. Use Discover to create one.')}")
    except Exception as exc:
        print(f"  {_red('✗')} {exc}")
    _pause()


# ─── Status ───────────────────────────────────────────────────────────────


def _show_status():
    _clear()
    print(_cyan(BANNER))
    print(f"  {_bold('📊 Status')}\n")

    from .connectors import CONNECTORS

    print(f"  {_bold('Available Connectors:')}")
    for name in CONNECTORS:
        print(f"    {_green('✓')} {name}")

    print(f"\n  {_bold('Configuration:')}")
    from .config import Settings

    s = Settings()
    print(f"    Database: {s.database_uri}")
    print(f"    LLM:      {s.llm_service}")
    print(f"    Data dir: {s.data_dir}")

    _pause()


# ─── Main ─────────────────────────────────────────────────────────────────


def main():
    try:
        while True:
            choice = _menu(
                "graffold-ingest — Turn anything into a knowledge graph",
                [
                    "① Fetch Data",
                    "② Extract Entities",
                    "③ Build Knowledge Graph",
                    "④ Schema Tools",
                    "⑤ Status",
                    "Quit",
                ],
                [
                    "web, PDF, API, CSV, database, smart agent",
                    "LLM-powered schema-free extraction",
                    "full pipeline: fetch → extract → publish",
                    "discover, validate, or refine your schema",
                    "connectors, config, pipeline state",
                    "",
                ],
            )

            if choice in (-1, 5):
                _clear()
                print(f"\n  {_dim('Bye!')}\n")
                break
            if choice == 0:
                _fetch_menu()
            elif choice == 1:
                _extract_menu()
            elif choice == 2:
                _build_menu()
            elif choice == 3:
                _schema_menu()
            elif choice == 4:
                _show_status()
    except KeyboardInterrupt:
        _clear()
        print(f"\n  {_dim('Bye!')}\n")


if __name__ == "__main__":
    main()
