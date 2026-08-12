"""Composite resolver — chains multiple resolvers together."""

from __future__ import annotations

from .base import BaseResolver, ResolvedEntity


class CompositeResolver:
    """Chains resolvers, returning the first successful resolution."""

    def __init__(self, resolvers: list[BaseResolver]) -> None:
        self._resolvers = resolvers

    async def resolve(self, name: str, label: str) -> ResolvedEntity | None:
        """Try each resolver that handles the label, return first match."""
        for resolver in self._resolvers:
            if resolver.handles(label):
                result = await resolver.resolve(name, label)
                if result:
                    return result
        return None
