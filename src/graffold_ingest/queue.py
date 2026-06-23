"""Async job queue for pipeline execution."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class Job:
    id: str
    tenant_id: str
    status: str = "pending"  # pending | running | completed | failed
    params: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] | None = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None


class InMemoryJobQueue:
    """Asyncio-based in-memory job queue with configurable concurrency."""

    def __init__(self, max_workers: int = 2) -> None:
        self._queue: asyncio.Queue[Job] = asyncio.Queue()
        self._jobs: dict[str, Job] = {}
        self._max_workers = max_workers
        self._tasks: list[asyncio.Task] = []

    async def enqueue(self, job_id: str, tenant_id: str, params: dict[str, Any]) -> Job:
        job = Job(id=job_id, tenant_id=tenant_id, params=params)
        self._jobs[job_id] = job
        await self._queue.put(job)
        return job

    def get_job(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def list_jobs(self, tenant_id: str) -> list[Job]:
        return [j for j in self._jobs.values() if j.tenant_id == tenant_id]

    def start_workers(self) -> None:
        for _ in range(self._max_workers):
            self._tasks.append(asyncio.create_task(self._worker()))

    async def _worker(self) -> None:
        while True:
            job = await self._queue.get()
            job.status = "running"
            job.started_at = time.time()
            logger.info("Running job %s for tenant %s", job.id, job.tenant_id)

            try:
                from .pipeline.orchestrator import run_pipeline

                result = await run_pipeline(
                    source=job.params["source"],
                    url=job.params.get("url", ""),
                    path=job.params.get("path", ""),
                    schema_path=job.params.get("schema_path"),
                    llm_service=job.params.get("llm_service", "bedrock"),
                    llm_model=job.params.get("llm_model", ""),
                    database_uri=job.params.get("database_uri", "bolt://localhost:7687"),
                    chunk_size=job.params.get("chunk_size", 2000),
                    tenant_id=job.params.get("tenant_id", job.tenant_id),
                    project_id=job.params.get("project_id", "default"),
                    incremental=job.params.get("incremental", True),
                )
                job.status = "completed"
                job.result = result.to_report()
            except Exception as e:
                job.status = "failed"
                job.error = str(e)
                logger.exception("Job %s failed", job.id)
            finally:
                job.completed_at = time.time()
                self._queue.task_done()


_queue: InMemoryJobQueue | None = None


def get_queue(max_workers: int = 2) -> InMemoryJobQueue:
    global _queue
    if _queue is None:
        _queue = InMemoryJobQueue(max_workers=max_workers)
    return _queue
