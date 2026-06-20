"""Base connector protocol and Document model."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field


class Document(BaseModel):
    """A fetched document ready for processing."""

    id: str
    content: str
    metadata: dict[str, Any] = {}
    source_url: str = ""
    source_type: str = ""  # "web", "pdf", "api", "csv", "database"
    title: str = ""
    chunk_id: str | None = None


class ExtractionResult(BaseModel):
    """Entities and relationships extracted from a document."""

    nodes: list[dict[str, Any]] = Field(default_factory=list)
    edges: list[dict[str, Any]] = Field(default_factory=list)
    source_doc_id: str = ""


@runtime_checkable
class Connector(Protocol):
    """Protocol for data source connectors."""

    def name(self) -> str: ...

    async def fetch(self, **kwargs: Any) -> list[Document]: ...
