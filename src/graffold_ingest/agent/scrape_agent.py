"""LLM-powered scraping agent."""

from __future__ import annotations


async def scrape_agent(
    url: str,
    depth: int = 1,
    llm_service: str = "bedrock",
) -> dict:
    """Autonomous scraping agent that discovers and follows relevant links.

    Given a starting URL, the agent:
    1. Fetches the page
    2. Asks the LLM what's relevant and what links to follow
    3. Recursively scrapes up to `depth` levels
    4. Returns all discovered documents

    This is the 'smart scraper' that goes beyond simple HTTP fetching.
    """
    from ..connectors.web import WebConnector
    from ..pipeline.chunk import chunk_documents
    from ..pipeline.extract import extract_entities

    connector = WebConnector()
    docs = await connector.fetch(url=url)

    # For depth > 1, would discover links and recurse
    # Stub for now — single-page extraction
    chunks = chunk_documents(docs)
    results = await extract_entities(chunks, llm_service=llm_service)

    total_nodes = sum(len(r.nodes) for r in results)
    total_edges = sum(len(r.edges) for r in results)

    return {
        "documents_fetched": len(docs),
        "chunks_processed": len(chunks),
        "entities_extracted": total_nodes,
        "relationships_extracted": total_edges,
    }
