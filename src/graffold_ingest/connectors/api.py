"""REST API connector."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import httpx

from .base import Document


class ApiConnector:
    """Fetch data from REST APIs."""

    def name(self) -> str:
        return "api"

    async def fetch(
        self,
        *,
        url: str = "",
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        paginate: bool = False,
        max_pages: int = 10,
        **kwargs: Any,
    ) -> list[Document]:
        """Fetch JSON data from a REST API endpoint."""
        if not url:
            return []

        docs: list[Document] = []
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers or {}, params=params or {})
            resp.raise_for_status()

            data = resp.json()
            content = json.dumps(data, indent=2) if isinstance(data, (dict, list)) else str(data)
            doc_id = hashlib.sha256(url.encode()).hexdigest()[:16]

            docs.append(
                Document(
                    id=doc_id,
                    content=content,
                    source_url=url,
                    source_type="api",
                    title=url.split("/")[-1] or url,
                    metadata={"status_code": resp.status_code},
                )
            )

        return docs
