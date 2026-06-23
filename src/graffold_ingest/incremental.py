"""Incremental ingestion — fingerprinting and change detection."""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .connectors.base import Document


@dataclass
class ContentFingerprint:
    doc_id: str
    content_hash: str
    source_url: str
    last_ingested_at: datetime
    chunk_count: int


class FingerprintStore(ABC):
    @abstractmethod
    def save_fingerprints(self, tenant_id: str, project_id: str, fingerprints: list[ContentFingerprint]) -> None: ...

    @abstractmethod
    def load_fingerprints(self, tenant_id: str, project_id: str) -> dict[str, ContentFingerprint]: ...


class FileFingerprintStore(FingerprintStore):
    """File-based fingerprint store: {base_dir}/{tenant}/{project}/fingerprints.json."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir

    def _path(self, tenant_id: str, project_id: str) -> Path:
        return self.base_dir / tenant_id / project_id / "fingerprints.json"

    def save_fingerprints(self, tenant_id: str, project_id: str, fingerprints: list[ContentFingerprint]) -> None:
        path = self._path(tenant_id, project_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            fp.doc_id: {**asdict(fp), "last_ingested_at": fp.last_ingested_at.isoformat()}
            for fp in fingerprints
        }
        path.write_text(json.dumps(data, indent=2))

    def load_fingerprints(self, tenant_id: str, project_id: str) -> dict[str, ContentFingerprint]:
        path = self._path(tenant_id, project_id)
        if not path.exists():
            return {}
        raw = json.loads(path.read_text())
        return {
            doc_id: ContentFingerprint(
                doc_id=entry["doc_id"],
                content_hash=entry["content_hash"],
                source_url=entry["source_url"],
                last_ingested_at=datetime.fromisoformat(entry["last_ingested_at"]),
                chunk_count=entry["chunk_count"],
            )
            for doc_id, entry in raw.items()
        }


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


def compute_fingerprint(doc: Document, chunk_count: int = 0) -> ContentFingerprint:
    return ContentFingerprint(
        doc_id=doc.id,
        content_hash=_sha256(doc.content),
        source_url=doc.source_url,
        last_ingested_at=datetime.now(timezone.utc),
        chunk_count=chunk_count,
    )


def filter_unchanged(
    documents: list[Document], existing: dict[str, ContentFingerprint]
) -> tuple[list[Document], list[Document]]:
    """Return (changed_docs, unchanged_docs). Only changed/new docs need processing."""
    changed, unchanged = [], []
    for doc in documents:
        h = _sha256(doc.content)
        if doc.id in existing and existing[doc.id].content_hash == h:
            unchanged.append(doc)
        else:
            changed.append(doc)
    return changed, unchanged
