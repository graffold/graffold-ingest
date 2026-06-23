"""Multi-tenant API layer for graffold-ingest."""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any, Protocol, runtime_checkable

from fastapi import Depends, FastAPI, HTTPException, Header
from pydantic import BaseModel


@runtime_checkable
class TenantStore(Protocol):
    def resolve(self, api_key: str) -> str | None: ...


class EnvTenantStore:
    """Resolve tenant from TENANT_KEYS env: 'sk-abc:tenant1,sk-xyz:tenant2'."""

    def __init__(self) -> None:
        raw = os.environ.get("TENANT_KEYS", "")
        self._map: dict[str, str] = {}
        for pair in raw.split(","):
            if ":" in pair:
                k, v = pair.split(":", 1)
                self._map[k.strip()] = v.strip()

    def resolve(self, api_key: str) -> str | None:
        return self._map.get(api_key)


_tenant_store: TenantStore = EnvTenantStore()


class IngestRequest(BaseModel):
    source: str
    url: str = ""
    path: str = ""
    project_id: str = "default"
    schema_path: str | None = None
    llm_service: str = "bedrock"
    llm_model: str = ""
    database_uri: str = "bolt://localhost:7687"
    chunk_size: int = 2000
    incremental: bool = True


class IngestResponse(BaseModel):
    job_id: str
    status: str


class JobResponse(BaseModel):
    job_id: str
    tenant_id: str
    status: str
    result: Any = None


def _get_tenant(authorization: str = Header(...)) -> str:
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "Invalid authorization header")
    token = authorization.removeprefix("Bearer ")
    tenant_id = _tenant_store.resolve(token)
    if tenant_id is None:
        raise HTTPException(403, "Unknown API key")
    return tenant_id


app = FastAPI(title="graffold-ingest")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest", response_model=IngestResponse)
async def ingest(req: IngestRequest, tenant_id: str = Depends(_get_tenant)):
    from .queue import get_queue

    job_id = str(uuid.uuid4())
    queue = get_queue()
    await queue.enqueue(
        job_id=job_id,
        tenant_id=tenant_id,
        params={
            "source": req.source,
            "url": req.url,
            "path": req.path,
            "schema_path": req.schema_path,
            "llm_service": req.llm_service,
            "llm_model": req.llm_model,
            "database_uri": req.database_uri,
            "chunk_size": req.chunk_size,
            "tenant_id": tenant_id,
            "project_id": req.project_id,
            "incremental": req.incremental,
        },
    )
    return IngestResponse(job_id=job_id, status="pending")


@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, tenant_id: str = Depends(_get_tenant)):
    from .queue import get_queue

    queue = get_queue()
    job = queue.get_job(job_id)
    if job is None or job.tenant_id != tenant_id:
        raise HTTPException(404, "Job not found")
    return JobResponse(
        job_id=job.id, tenant_id=job.tenant_id, status=job.status, result=job.result
    )


@app.on_event("startup")
async def _start_workers():
    from .queue import get_queue

    queue = get_queue()
    queue.start_workers()
