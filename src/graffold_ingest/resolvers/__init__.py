"""Pluggable entity resolution framework."""

from .base import BaseResolver, ResolvedEntity
from .composite import CompositeResolver
from .enhanced import resolve_entities_enhanced
from .mondo import MONDOResolver
from .pubchem import PubChemResolver
from .uniprot import UniProtResolver

__all__ = [
    "BaseResolver",
    "CompositeResolver",
    "MONDOResolver",
    "PubChemResolver",
    "ResolvedEntity",
    "UniProtResolver",
    "resolve_entities_enhanced",
]
