"""Schema versioning — store and track schema iterations per project."""

from __future__ import annotations

import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml


@dataclass
class SchemaVersion:
    version_id: str
    tenant_id: str
    project_id: str
    schema_yaml: str
    created_at: datetime
    parent_version_id: str | None = None
    description: str = ""


class SchemaStore(ABC):
    @abstractmethod
    def save(self, tenant_id: str, project_id: str, schema_yaml: str, description: str = "") -> SchemaVersion: ...

    @abstractmethod
    def get(self, tenant_id: str, project_id: str, version_id: str | None = None) -> SchemaVersion | None: ...

    @abstractmethod
    def list_versions(self, tenant_id: str, project_id: str) -> list[SchemaVersion]: ...

    @abstractmethod
    def diff(self, tenant_id: str, project_id: str, v1: str, v2: str) -> dict: ...


class FileSchemaStore(SchemaStore):
    """File-based schema version store: {base_dir}/{tenant}/{project}/v{n}.yaml + meta.json."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir

    def _project_dir(self, tenant_id: str, project_id: str) -> Path:
        return self.base_dir / tenant_id / project_id

    def _meta_path(self, tenant_id: str, project_id: str) -> Path:
        return self._project_dir(tenant_id, project_id) / "meta.json"

    def _load_meta(self, tenant_id: str, project_id: str) -> list[dict]:
        path = self._meta_path(tenant_id, project_id)
        if not path.exists():
            return []
        return json.loads(path.read_text())

    def _save_meta(self, tenant_id: str, project_id: str, meta: list[dict]) -> None:
        path = self._meta_path(tenant_id, project_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(meta, indent=2, default=str))

    def save(self, tenant_id: str, project_id: str, schema_yaml: str, description: str = "") -> SchemaVersion:
        meta = self._load_meta(tenant_id, project_id)
        n = len(meta) + 1
        version_id = str(uuid.uuid4())
        parent = meta[-1]["version_id"] if meta else None

        version = SchemaVersion(
            version_id=version_id,
            tenant_id=tenant_id,
            project_id=project_id,
            schema_yaml=schema_yaml,
            created_at=datetime.now(timezone.utc),
            parent_version_id=parent,
            description=description,
        )

        # Write schema file
        d = self._project_dir(tenant_id, project_id)
        d.mkdir(parents=True, exist_ok=True)
        (d / f"v{n}.yaml").write_text(schema_yaml)

        # Update meta
        meta.append({
            "version_id": version_id,
            "n": n,
            "created_at": version.created_at.isoformat(),
            "parent_version_id": parent,
            "description": description,
        })
        self._save_meta(tenant_id, project_id, meta)
        return version

    def get(self, tenant_id: str, project_id: str, version_id: str | None = None) -> SchemaVersion | None:
        meta = self._load_meta(tenant_id, project_id)
        if not meta:
            return None

        entry = None
        if version_id:
            entry = next((m for m in meta if m["version_id"] == version_id), None)
        else:
            entry = meta[-1]

        if not entry:
            return None

        schema_file = self._project_dir(tenant_id, project_id) / f"v{entry['n']}.yaml"
        if not schema_file.exists():
            return None

        return SchemaVersion(
            version_id=entry["version_id"],
            tenant_id=tenant_id,
            project_id=project_id,
            schema_yaml=schema_file.read_text(),
            created_at=datetime.fromisoformat(entry["created_at"]),
            parent_version_id=entry.get("parent_version_id"),
            description=entry.get("description", ""),
        )

    def list_versions(self, tenant_id: str, project_id: str) -> list[SchemaVersion]:
        meta = self._load_meta(tenant_id, project_id)
        versions = []
        for entry in meta:
            v = self.get(tenant_id, project_id, entry["version_id"])
            if v:
                versions.append(v)
        return versions

    def diff(self, tenant_id: str, project_id: str, v1: str, v2: str) -> dict:
        s1 = self.get(tenant_id, project_id, v1)
        s2 = self.get(tenant_id, project_id, v2)
        if not s1 or not s2:
            return {"error": "Version not found"}

        d1 = yaml.safe_load(s1.schema_yaml) or {}
        d2 = yaml.safe_load(s2.schema_yaml) or {}

        entities_1 = {e["name"] for e in d1.get("entities", [])}
        entities_2 = {e["name"] for e in d2.get("entities", [])}
        rels_1 = {r["type"] for r in d1.get("relationships", [])}
        rels_2 = {r["type"] for r in d2.get("relationships", [])}

        return {
            "entities_added": sorted(entities_2 - entities_1),
            "entities_removed": sorted(entities_1 - entities_2),
            "relationships_added": sorted(rels_2 - rels_1),
            "relationships_removed": sorted(rels_1 - rels_2),
        }
