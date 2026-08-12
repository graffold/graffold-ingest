"""Base resolver protocol and resolved entity model."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class ResolvedEntity:
    """A canonical entity resolved from an external authority."""

    canonical_id: str
    canonical_name: str
    source_names: list[str] = field(default_factory=list)
    resolver: str = ""
    confidence: float = 1.0
    metadata: dict = field(default_factory=dict)


class BaseResolver(ABC):
    """Abstract base class for entity resolvers."""

    @abstractmethod
    async def resolve(self, name: str, label: str) -> ResolvedEntity | None:
        """Try to resolve an entity name to a canonical form."""
        ...

    @abstractmethod
    def handles(self, label: str) -> bool:
        """Return True if this resolver handles entities with this label."""
        ...
