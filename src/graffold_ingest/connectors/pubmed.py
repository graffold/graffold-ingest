"""PubMed connector — fetch abstracts via NCBI E-utilities.

Native graffold-ingest connector: fetch(query=...) → list[Document].
No API key required for low volume; set NCBI_API_KEY / ENTREZ_API_KEY for higher rate.

Usage:
    connector = PubMedConnector()
    docs = await connector.fetch(query="F18 ETEC adhesion piglet", limit=25)
    docs = await connector.fetch(pmids=["40304242", "38123456"])
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

from .base import Document

logger = logging.getLogger(__name__)

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


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
        ids = list(pmids or [])

        async with httpx.AsyncClient(timeout=30.0) as client:
            # Search for PMIDs if query given
            if query:
                params = {"db": "pubmed", "term": query, "retmax": limit, "retmode": "json"}
                if api_key:
                    params["api_key"] = api_key
                try:
                    resp = await client.get(f"{EUTILS}/esearch.fcgi", params=params)
                    resp.raise_for_status()
                    found = resp.json().get("esearchresult", {}).get("idlist", [])
                    ids.extend(found)
                except Exception as e:
                    logger.warning("PubMed search failed: %s", e)

            if not ids:
                return []

            ids = ids[:limit]

            # Fetch abstracts (efetch with rettype=abstract, retmode=xml → parse)
            params = {"db": "pubmed", "id": ",".join(ids), "rettype": "abstract", "retmode": "xml"}
            if api_key:
                params["api_key"] = api_key
            try:
                resp = await client.get(f"{EUTILS}/efetch.fcgi", params=params)
                resp.raise_for_status()
                return _parse_pubmed_xml(resp.text)
            except Exception as e:
                logger.warning("PubMed fetch failed: %s", e)
                return []


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

        # Abstract can have multiple <AbstractText> sections
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

        # Journal + year for provenance
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
