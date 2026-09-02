"""PubMed connector — fetch abstracts via NCBI E-utilities.

Native graffold-ingest connector: fetch(query=...) → list[Document].
Rate-limited + retried to respect NCBI limits (3 req/s, 10/s with API key).
Set NCBI_API_KEY / ENTREZ_API_KEY for higher throughput.

Usage:
    connector = PubMedConnector()
    docs = await connector.fetch(query="F18 ETEC adhesion piglet", limit=25)
    docs = await connector.fetch(pmids=["40304242", "38123456"])
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

import httpx

from .base import Document

logger = logging.getLogger(__name__)

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

# NCBI: 3 req/s without a key, 10/s with one. Throttle conservatively.
_last_request_time = 0.0
_rate_lock = asyncio.Lock()


async def _throttle(has_key: bool) -> None:
    """Space out NCBI requests to respect rate limits."""
    global _last_request_time
    min_interval = 0.11 if has_key else 0.34  # ~9/s or ~3/s
    async with _rate_lock:
        elapsed = time.monotonic() - _last_request_time
        if elapsed < min_interval:
            await asyncio.sleep(min_interval - elapsed)
        _last_request_time = time.monotonic()


async def _get_with_retry(
    client: httpx.AsyncClient, url: str, params: dict, has_key: bool, retries: int = 4
) -> httpx.Response | None:
    """GET with throttle + exponential backoff on 429/502/503."""
    for attempt in range(retries):
        await _throttle(has_key)
        try:
            resp = await client.get(url, params=params)
            if resp.status_code in (429, 502, 503):
                wait = 2**attempt
                logger.warning("NCBI %d — backing off %ds", resp.status_code, wait)
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError:
            return None
        except Exception as e:
            logger.warning("NCBI request error: %s", e)
            await asyncio.sleep(2**attempt)
    return None


class PubMedConnector:
    """Fetch PubMed abstracts as Documents for KG extraction."""

    def name(self) -> str:
        return "pubmed"

    async def fetch(
        self,
        *,
        query: str = "",
        pmids: list[str] | None = None,
        limit: int = 25,
        **kwargs: Any,
    ) -> list[Document]:
        """Fetch abstracts by search query and/or explicit PMIDs."""
        api_key = os.getenv("NCBI_API_KEY") or os.getenv("ENTREZ_API_KEY", "")
        has_key = bool(api_key)
        ids = list(pmids or [])

        async with httpx.AsyncClient(timeout=30.0) as client:
            if query:
                params = {"db": "pubmed", "term": query, "retmax": limit, "retmode": "json"}
                if api_key:
                    params["api_key"] = api_key
                resp = await _get_with_retry(client, f"{EUTILS}/esearch.fcgi", params, has_key)
                if resp is not None:
                    try:
                        found = resp.json().get("esearchresult", {}).get("idlist", [])
                        ids.extend(found)
                    except Exception:
                        pass

            if not ids:
                return []

            ids = ids[:limit]
            params = {"db": "pubmed", "id": ",".join(ids), "rettype": "abstract", "retmode": "xml"}
            if api_key:
                params["api_key"] = api_key
            resp = await _get_with_retry(client, f"{EUTILS}/efetch.fcgi", params, has_key)
            if resp is None:
                return []
            return _parse_pubmed_xml(resp.text)


def _parse_pubmed_xml(xml_text: str) -> list[Document]:
    """Parse PubMed efetch XML into Documents."""
    import xml.etree.ElementTree as ET

    docs: list[Document] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return docs

    for article in root.findall(".//PubmedArticle"):
        pmid_el = article.find(".//PMID")
        pmid = pmid_el.text if pmid_el is not None else ""

        title_el = article.find(".//ArticleTitle")
        title = title_el.text if title_el is not None else ""

        abstract_parts = []
        for abs_el in article.findall(".//AbstractText"):
            label = abs_el.get("Label", "")
            text = "".join(abs_el.itertext())
            if label:
                abstract_parts.append(f"{label}: {text}")
            else:
                abstract_parts.append(text)
        abstract = "\n".join(abstract_parts)

        if not abstract:
            continue

        journal_el = article.find(".//Journal/Title")
        journal = journal_el.text if journal_el is not None else ""
        year_el = article.find(".//PubDate/Year")
        year = year_el.text if year_el is not None else ""

        docs.append(Document(
            id=f"pmid:{pmid}",
            content=f"{title}\n\n{abstract}",
            source_url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            source_type="pubmed",
            title=title or f"PMID {pmid}",
            metadata={"pmid": pmid, "journal": journal, "year": year},
        ))

    return docs
