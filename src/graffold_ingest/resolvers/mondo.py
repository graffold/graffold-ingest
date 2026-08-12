"""MONDO resolver — canonicalize Disease/Condition/Disorder entities."""

from __future__ import annotations

import logging

import httpx

from .base import BaseResolver, ResolvedEntity

logger = logging.getLogger(__name__)

_HANDLED_LABELS = {"disease", "condition", "disorder"}

_cache: dict[str, ResolvedEntity | None] = {}


class MONDOResolver(BaseResolver):
    """Resolve disease names against the MONDO ontology via OLS4 API."""

    def handles(self, label: str) -> bool:
        return label.lower() in _HANDLED_LABELS

    async def resolve(self, name: str, label: str) -> ResolvedEntity | None:
        """Search OLS4 for a disease name and return canonical MONDO ID."""
        cache_key = name.lower().strip()
        if cache_key in _cache:
            return _cache[cache_key]

        url = "https://www.ebi.ac.uk/ols4/api/search"
        params = {
            "q": name,
            "ontology": "mondo",
            "rows": "1",
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            logger.warning("MONDO lookup failed for %r: %s", name, exc)
            _cache[cache_key] = None
            return None

        data = resp.json()
        docs = data.get("response", {}).get("docs", [])
        if not docs:
            _cache[cache_key] = None
            return None

        doc = docs[0]
        mondo_id = doc.get("obo_id", "")
        preferred_label = doc.get("label", name)
        synonyms = doc.get("synonym", [])

        resolved = ResolvedEntity(
            canonical_id=mondo_id,
            canonical_name=preferred_label,
            source_names=[name],
            resolver="mondo",
            confidence=0.85,
            metadata={"synonyms": synonyms},
        )
        _cache[cache_key] = resolved
        return resolved
