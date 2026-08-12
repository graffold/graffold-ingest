"""UniProt resolver — canonicalize Protein/Target/Enzyme entities."""

from __future__ import annotations

import logging

import httpx

from .base import BaseResolver, ResolvedEntity

logger = logging.getLogger(__name__)

_HANDLED_LABELS = {"protein", "target", "enzyme"}

_cache: dict[str, ResolvedEntity | None] = {}


class UniProtResolver(BaseResolver):
    """Resolve protein names against UniProt REST API."""

    def handles(self, label: str) -> bool:
        return label.lower() in _HANDLED_LABELS

    async def resolve(self, name: str, label: str) -> ResolvedEntity | None:
        """Search UniProt for a protein name and return canonical info."""
        cache_key = name.lower().strip()
        if cache_key in _cache:
            return _cache[cache_key]

        url = "https://rest.uniprot.org/uniprotkb/search"
        params = {
            "query": name,
            "fields": "accession,protein_name,organism_name",
            "size": "1",
            "format": "json",
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            logger.warning("UniProt lookup failed for %r: %s", name, exc)
            _cache[cache_key] = None
            return None

        data = resp.json()
        results = data.get("results", [])
        if not results:
            _cache[cache_key] = None
            return None

        entry = results[0]
        accession = entry.get("primaryAccession", "")
        protein_desc = entry.get("proteinDescription", {})
        rec_name = protein_desc.get("recommendedName", {})
        full_name = rec_name.get("fullName", {}).get("value", name)
        organism = entry.get("organism", {}).get("scientificName", "")

        resolved = ResolvedEntity(
            canonical_id=accession,
            canonical_name=full_name,
            source_names=[name],
            resolver="uniprot",
            confidence=0.9,
            metadata={"organism": organism},
        )
        _cache[cache_key] = resolved
        return resolved
