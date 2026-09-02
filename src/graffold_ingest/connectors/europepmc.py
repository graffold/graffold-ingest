"""Europe PMC connector — fetch open-access full-text and abstracts.

Native graffold-ingest connector: fetch(query=...) → list[Document].
Europe PMC has a free REST API (no key) covering PubMed + PMC + preprints.

Usage:
    connector = EuropePMCConnector()
    docs = await connector.fetch(query="F18 ETEC virulence", limit=25)
    docs = await connector.fetch(query="ETEC", full_text=True)  # prefer OA full text
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from .base import Document

logger = logging.getLogger(__name__)

BASE = "https://www.ebi.ac.uk/europepmc/webservices/rest"


class EuropePMCConnector:
    """Fetch Europe PMC abstracts + open-access full-text as Documents."""

    def name(self) -> str:
        return "europepmc"

    async def fetch(
        self,
        *,
        query: str = "",
        limit: int = 25,
        full_text: bool = False,
        **kwargs: Any,
    ) -> list[Document]:
        """Search Europe PMC and return Documents.

        Args:
            query: Search query.
            limit: Max results.
            full_text: If True, fetch OA full-text where available (slower).
        """
        if not query:
            return []

        docs: list[Document] = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            params = {
                "query": query,
                "format": "json",
                "pageSize": min(limit, 100),
                "resultType": "core",
            }
            results = []
            for attempt in range(3):
                try:
                    resp = await client.get(f"{BASE}/search", params=params)
                    if resp.status_code in (429, 502, 503):
                        await asyncio.sleep(2**attempt)
                        continue
                    resp.raise_for_status()
                    results = resp.json().get("resultList", {}).get("result", [])
                    break
                except Exception as e:
                    logger.warning("Europe PMC search failed (attempt %d): %s", attempt + 1, str(e)[:80])
                    await asyncio.sleep(2**attempt)
            if not results:
                return []

            for r in results[:limit]:
                pmid = r.get("pmid", "")
                pmcid = r.get("pmcid", "")
                title = r.get("title", "")
                abstract = r.get("abstractText", "")
                content = f"{title}\n\n{abstract}" if abstract else title

                # Optionally fetch OA full text
                if full_text and pmcid and r.get("isOpenAccess") == "Y":
                    ft = await _fetch_fulltext(client, pmcid)
                    if ft:
                        content = f"{title}\n\n{ft}"

                if not content.strip():
                    continue

                docs.append(Document(
                    id=f"pmid:{pmid}" if pmid else f"pmcid:{pmcid}",
                    content=content,
                    source_url=f"https://europepmc.org/article/MED/{pmid}" if pmid else "",
                    source_type="europepmc",
                    title=title or (pmid or pmcid),
                    metadata={
                        "pmid": pmid,
                        "pmcid": pmcid,
                        "year": r.get("pubYear", ""),
                        "journal": r.get("journalTitle", ""),
                        "is_open_access": r.get("isOpenAccess") == "Y",
                    },
                ))

        return docs


async def _fetch_fulltext(client: httpx.AsyncClient, pmcid: str) -> str:
    """Fetch OA full-text XML and extract plain text."""
    try:
        resp = await client.get(f"{BASE}/{pmcid}/fullTextXML")
        if resp.status_code != 200:
            return ""
        import xml.etree.ElementTree as ET

        root = ET.fromstring(resp.text)
        # Extract body text, skip references
        body = root.find(".//body")
        if body is None:
            return ""
        return " ".join(body.itertext())[:20000]  # cap at 20k chars
    except Exception:
        return ""
