"""Web scraping connector."""

from __future__ import annotations

import hashlib
from typing import Any

import httpx

from .base import Document


class WebConnector:
    """Fetch content from web pages."""

    def name(self) -> str:
        return "web"

    async def fetch(self, *, urls: list[str] | None = None, url: str = "", **kwargs: Any) -> list[Document]:
        """Fetch web pages and return as Documents."""
        targets = urls or ([url] if url else [])
        docs: list[Document] = []

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            for target_url in targets:
                try:
                    resp = await client.get(target_url)
                    resp.raise_for_status()
                    doc_id = hashlib.sha256(target_url.encode()).hexdigest()[:16]
                    docs.append(
                        Document(
                            id=doc_id,
                            content=resp.text,
                            source_url=target_url,
                            source_type="web",
                            title=target_url.split("/")[-1] or target_url,
                        )
                    )
                except Exception:
                    pass  # Skip failed URLs

        return docs
