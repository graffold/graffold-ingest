"""Tests for POST /v1/entities endpoint."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from graffold_ingest.api import app


@pytest.fixture()
def client():
    """Synchronous test client (uses internal mode by default)."""
    return TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entities(n: int = 3) -> list[dict]:
    return [
        {"name": f"Entity{i}", "label": "Concept", "properties": {"rank": i}}
        for i in range(n)
    ]


def _make_relationships(n: int = 2) -> list[dict]:
    return [
        {
            "source": f"entity-{i}",
            "target": f"entity-{i + 1}",
            "type": "RELATES_TO",
        }
        for i in range(n)
    ]


def _base_payload(**overrides) -> dict:
    payload = {
        "source_run_id": "run-abc-123",
        "source_system": "agteria",
        "project_id": "proj-1",
        "entities": _make_entities(3),
        "relationships": _make_relationships(2),
    }
    payload.update(overrides)
    return payload


# ---------------------------------------------------------------------------
# Sync path (< 50 entities)
# ---------------------------------------------------------------------------


@patch("graffold_ingest.pipeline.entity_push.check_idempotency", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.entity_push.mark_processed", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.entity_push.embed_and_upload", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.entity_push.publish_to_graph", new_callable=AsyncMock)
def test_push_entities_sync_success(
    mock_publish, mock_embed, mock_mark, mock_idemp, client
):
    """Small payload is processed synchronously and returns completed."""
    # Not yet processed
    mock_idemp.return_value = False
    mock_publish.return_value = {"nodes_created": 3, "edges_created": 2}
    mock_embed.return_value = 3

    resp = client.post("/v1/entities", json=_base_payload())

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["job_id"]  # non-empty UUID
    assert body["nodes_created"] == 3
    assert body["edges_created"] == 2
    assert body["embeddings_queued"] == 3


@patch("graffold_ingest.pipeline.entity_push.check_idempotency", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.entity_push.mark_processed", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.entity_push.embed_and_upload", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.entity_push.publish_to_graph", new_callable=AsyncMock)
def test_push_entities_idempotent(
    mock_publish, mock_embed, mock_mark, mock_idemp, client
):
    """Re-pushing the same source_run_id is a no-op."""
    mock_idemp.return_value = True  # Already processed

    resp = client.post("/v1/entities", json=_base_payload())

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["nodes_created"] == 0
    assert body["edges_created"] == 0
    assert body["embeddings_queued"] == 0

    # publish should never be called
    mock_publish.assert_not_called()


# ---------------------------------------------------------------------------
# Async path (>= 50 entities)
# ---------------------------------------------------------------------------


@patch("graffold_ingest.queue.get_queue")
def test_push_entities_async_large_payload(mock_get_queue, client):
    """Large payload (>=50 entities) is enqueued and returns accepted."""
    mock_queue = AsyncMock()
    mock_get_queue.return_value = mock_queue

    payload = _base_payload(entities=_make_entities(60))

    resp = client.post("/v1/entities", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["job_id"]
    assert body["nodes_created"] == 0  # not yet processed

    # Verify job was enqueued
    mock_queue.enqueue.assert_called_once()
    call_kwargs = mock_queue.enqueue.call_args.kwargs
    assert call_kwargs["params"]["_task"] == "entity_push"
    assert call_kwargs["params"]["source_run_id"] == "run-abc-123"
    assert len(call_kwargs["params"]["entities"]) == 60


# ---------------------------------------------------------------------------
# Validation & auth
# ---------------------------------------------------------------------------


def test_push_entities_missing_required_field(client):
    """Missing source_run_id should return 422."""
    payload = {"entities": _make_entities(2)}  # no source_run_id

    resp = client.post("/v1/entities", json=payload)
    assert resp.status_code == 422


def test_push_entities_empty_entities_list(client):
    """Empty entities list should still succeed (sync path, no-op)."""
    payload = _base_payload(entities=[])

    with patch(
        "graffold_ingest.pipeline.entity_push.check_idempotency",
        new_callable=AsyncMock,
        return_value=False,
    ), patch(
        "graffold_ingest.pipeline.entity_push.mark_processed",
        new_callable=AsyncMock,
    ), patch(
        "graffold_ingest.pipeline.entity_push.embed_and_upload",
        new_callable=AsyncMock,
        return_value=0,
    ), patch(
        "graffold_ingest.pipeline.entity_push.publish_to_graph",
        new_callable=AsyncMock,
        return_value={"nodes_created": 0, "edges_created": 0},
    ):
        resp = client.post("/v1/entities", json=payload)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["nodes_created"] == 0


@patch("graffold_ingest.api._INTERNAL_MODE", False)
def test_push_entities_auth_required(client):
    """Without internal mode, auth is required."""
    resp = client.post("/v1/entities", json=_base_payload())
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Embed failure is non-fatal
# ---------------------------------------------------------------------------


@patch("graffold_ingest.pipeline.entity_push.check_idempotency", new_callable=AsyncMock)
@patch("graffold_ingest.pipeline.entity_push.mark_processed", new_callable=AsyncMock)
@patch(
    "graffold_ingest.pipeline.entity_push.embed_and_upload",
    new_callable=AsyncMock,
    side_effect=RuntimeError("CF API down"),
)
@patch("graffold_ingest.pipeline.entity_push.publish_to_graph", new_callable=AsyncMock)
def test_push_entities_embed_failure_non_fatal(
    mock_publish, mock_embed, mock_mark, mock_idemp, client
):
    """Embedding failures don't fail the overall push."""
    mock_idemp.return_value = False
    mock_publish.return_value = {"nodes_created": 3, "edges_created": 2}

    resp = client.post("/v1/entities", json=_base_payload())

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["embeddings_queued"] == 0  # failed gracefully
    assert body["nodes_created"] == 3
