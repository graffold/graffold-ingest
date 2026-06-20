"""Deduplication — checks graph DB for already-ingested documents.

Prevents re-processing documents that have already been ingested.
Uses content hashing to identify duplicates regardless of source type.
"""

from __future__ import annotations

import hashlib
from typing import Any

from ..connectors.base import Document


def content_hash(content: str) -> str:
    """SHA-256 hash of content (first 100KB for large docs)."""
    return hashlib.sha256(content[:102400].encode()).hexdigest()[:16]


class Deduplicator:
    """Filters out already-ingested documents by checking the graph DB."""

    def __init__(self, driver: Any = None, database: str = "neo4j"):
        self._driver = driver
        self._database = database

    def get_existing_hashes(self) -> set[str]:
        """Query all existing _version_hash values from the graph."""
        if not self._driver:
            return set()
        try:
            with self._driver.session(database=self._database) as session:
                result = session.run(
                    "MATCH (n) WHERE n._version_hash IS NOT NULL "
                    "RETURN DISTINCT n._version_hash AS h"
                )
                return {r["h"] for r in result}
        except Exception:
            return set()

    def filter_new(self, documents: list[Document], force: bool = False) -> list[Document]:
        """Return only documents not yet in the graph."""
        if force or not self._driver:
            return documents
        existing = self.get_existing_hashes()
        if not existing:
            return documents
        return [d for d in documents if content_hash(d.content) not in existing]

    def mark_processed(self, doc_id: str) -> None:
        """Mark a document as fully processed in the graph."""
        if not self._driver:
            return
        try:
            with self._driver.session(database=self._database) as session:
                session.run(
                    "MERGE (d:ProcessedDocument {doc_id: $id}) "
                    "SET d.processed_at = $ts",
                    {"id": doc_id, "ts": __import__("time").time()},
                )
        except Exception:
            pass
