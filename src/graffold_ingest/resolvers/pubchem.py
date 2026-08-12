"""PubChem resolver — canonicalize Compound/Drug/Molecule/Chemical entities."""

from __future__ import annotations

import logging
from urllib.parse import quote

import httpx

from .base import BaseResolver, ResolvedEntity

logger = logging.getLogger(__name__)

_HANDLED_LABELS = {"compound", "drug", "molecule", "chemical"}

_cache: dict[str, ResolvedEntity | None] = {}


class PubChemResolver(BaseResolver):
    """Resolve compound names against PubChem PUG REST API."""

    def handles(self, label: str) -> bool:
        return label.lower() in _HANDLED_LABELS

    async def resolve(self, name: str, label: str) -> ResolvedEntity | None:
        """Search PubChem for a compound name and return CID + properties."""
        cache_key = name.lower().strip()
        if cache_key in _cache:
            return _cache[cache_key]

        encoded_name = quote(name, safe="")
        url = (
            f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/"
            f"{encoded_name}/property/IUPACName,MolecularFormula,CID/JSON"
        )

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url)
                resp.raise_for_status()
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            logger.warning("PubChem lookup failed for %r: %s", name, exc)
            _cache[cache_key] = None
            return None

        data = resp.json()
        properties = data.get("PropertyTable", {}).get("Properties", [])
        if not properties:
            _cache[cache_key] = None
            return None

        prop = properties[0]
        cid = str(prop.get("CID", ""))
        iupac_name = prop.get("IUPACName", name)
        formula = prop.get("MolecularFormula", "")

        resolved = ResolvedEntity(
            canonical_id=f"CID:{cid}",
            canonical_name=iupac_name,
            source_names=[name],
            resolver="pubchem",
            confidence=0.9,
            metadata={"cid": cid, "molecular_formula": formula},
        )
        _cache[cache_key] = resolved
        return resolved
