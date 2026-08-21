"""AWS Neptune backend — OpenCypher over HTTPS with IAM SigV4 auth.

Neptune supports OpenCypher (same syntax as Neo4j) via HTTPS endpoint.
Auth is IAM-based using SigV4 signing.

Configuration:
    NEPTUNE_ENDPOINT: Neptune cluster endpoint (e.g. my-cluster.us-east-1.neptune.amazonaws.com)
    NEPTUNE_PORT: Port (default: 8182)
    AWS_REGION: AWS region for SigV4 signing
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any

from ...connectors.base import ExtractionResult

logger = logging.getLogger(__name__)


class NeptuneBackend:
    """Graph backend using AWS Neptune via OpenCypher HTTPS endpoint."""

    def __init__(
        self,
        endpoint: str | None = None,
        port: int | None = None,
        region: str | None = None,
        use_iam: bool = True,
        **kwargs: Any,
    ) -> None:
        self._endpoint = endpoint or os.getenv("NEPTUNE_ENDPOINT", "")
        self._port = port or int(os.getenv("NEPTUNE_PORT", "8182"))
        self._region = region or os.getenv("AWS_REGION", "us-east-1")
        self._use_iam = use_iam
        self._base_url = f"https://{self._endpoint}:{self._port}"

    @property
    def name(self) -> str:
        return "neptune"

    async def publish(
        self,
        results: list[ExtractionResult],
        **kwargs: Any,
    ) -> dict[str, int]:
        """Publish entities/relationships via OpenCypher MERGE statements."""
        import httpx

        nodes_created = 0
        edges_created = 0
        ingested_at = int(time.time() * 1000)

        async with httpx.AsyncClient(timeout=60.0) as client:
            for result in results:
                version_hash = hashlib.sha256(
                    result.source_doc_id.encode()
                ).hexdigest()[:12]

                # Publish nodes
                for node in result.nodes:
                    label = node.get("label", node.get("type", "Entity"))
                    props = {
                        k: v for k, v in node.items()
                        if k not in ("id", "label", "type") and v is not None
                    }
                    props["name"] = node.get("name", node.get("id", ""))
                    props["_source_doc_id"] = result.source_doc_id
                    props["_ingested_at"] = ingested_at
                    props["_extraction_method"] = "llm"
                    props["_version_hash"] = version_hash

                    cypher = (
                        f"MERGE (n:`{label}` {{id: $id}}) "
                        f"SET n += $props"
                    )
                    resp = await self._execute_cypher(
                        client, cypher, {"id": node["id"], "props": props}
                    )
                    if resp:
                        nodes_created += 1

                # Publish edges
                for edge in result.edges:
                    rel_type = edge.get("type", "RELATED_TO")
                    props = {
                        k: v for k, v in edge.get("properties", {}).items()
                        if v is not None
                    }
                    props["_source_doc_id"] = result.source_doc_id
                    props["_ingested_at"] = ingested_at

                    cypher = (
                        "MATCH (a {id: $source}), (b {id: $target}) "
                        f"MERGE (a)-[r:`{rel_type}`]->(b) "
                        "SET r += $props"
                    )
                    source_id = edge.get("source_id", edge.get("source", ""))
                    target_id = edge.get("target_id", edge.get("target", ""))
                    resp = await self._execute_cypher(
                        client, cypher,
                        {"source": source_id, "target": target_id, "props": props},
                    )
                    if resp:
                        edges_created += 1

        return {"nodes_created": nodes_created, "edges_created": edges_created}

    async def query_entities(
        self,
        search_term: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        import httpx

        cypher = (
            "MATCH (e) WHERE toLower(e.name) CONTAINS toLower($term) "
            "RETURN e.id AS id, e.name AS name, labels(e) AS labels "
            "LIMIT $limit"
        )
        async with httpx.AsyncClient(timeout=30.0) as client:
            result = await self._execute_cypher(
                client, cypher, {"term": search_term, "limit": limit}
            )
            return result.get("results", []) if result else []

    async def get_neighbors(
        self,
        entity_id: str,
        *,
        max_hops: int = 1,
    ) -> dict[str, Any]:
        import httpx

        cypher = (
            "MATCH (e {id: $id})-[r*1..$hops]-(n) "
            "RETURN e.name AS source, collect(DISTINCT n.name) AS neighbors"
        )
        async with httpx.AsyncClient(timeout=30.0) as client:
            result = await self._execute_cypher(
                client, cypher, {"id": entity_id, "hops": max_hops}
            )
            return result if result else {"neighbors": []}

    async def health_check(self) -> bool:
        import httpx

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(f"{self._base_url}/status")
                return resp.status_code == 200
        except Exception:
            return False

    # ─── Internal ──────────────────────────────────────────────────────────────

    async def _execute_cypher(
        self,
        client: Any,
        cypher: str,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Execute OpenCypher query against Neptune HTTPS endpoint."""
        url = f"{self._base_url}/openCypher"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        if self._use_iam:
            headers.update(self._sign_request(url, "POST"))

        data = {"query": cypher}
        if parameters:
            data["parameters"] = json.dumps(parameters)

        try:
            resp = await client.post(url, data=data, headers=headers)
            if resp.status_code == 200:
                return resp.json()
            logger.warning("Neptune query failed (%d): %s", resp.status_code, resp.text[:200])
            return None
        except Exception as e:
            logger.warning("Neptune request failed: %s", e)
            return None

    def _sign_request(self, url: str, method: str) -> dict[str, str]:
        """Generate SigV4 headers for Neptune IAM auth.

        Requires boto3 for credential resolution.
        """
        try:
            from botocore.auth import SigV4Auth
            from botocore.awsrequest import AWSRequest
            from botocore.session import Session

            session = Session()
            credentials = session.get_credentials().get_frozen_credentials()
            request = AWSRequest(method=method, url=url)
            SigV4Auth(credentials, "neptune-db", self._region).add_auth(request)
            return dict(request.headers)
        except ImportError:
            logger.warning("boto3/botocore not installed, skipping IAM auth")
            return {}
        except Exception as e:
            logger.warning("SigV4 signing failed: %s", e)
            return {}
