# Copyright 2026 Cloud-Dog, Viewdeck Engineering Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# cloud_dog_vdb — Remote client-only VDB proxy
"""HTTP proxy client for remote VDB services."""

from __future__ import annotations

from dataclasses import asdict

import httpx

from cloud_dog_vdb.compat.response_normaliser import ResponseNormaliser
from cloud_dog_vdb.domain.models import Record, SearchRequest, SearchResponse


class VDBClient:
    """Remote-only client that proxies operations to a hosted VDB service."""

    def __init__(
        self,
        remote_url: str,
        *,
        backend_hint: str = "portable",
        timeout_seconds: float = 30.0,
    ) -> None:
        self._remote_url = remote_url.rstrip("/")
        self._backend_hint = backend_hint
        self._normaliser = ResponseNormaliser()
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def health_check(self) -> bool:
        """Handle health check."""
        try:
            resp = await self._client.get(f"{self._remote_url}/health")
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

    async def search(self, collection: str, request: SearchRequest) -> SearchResponse:
        """Handle search."""
        payload = asdict(request)
        try:
            resp = await self._client.post(
                f"{self._remote_url}/collections/{collection}/search",
                json=payload,
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Remote search failed for {collection}") from exc
        backend = resp.headers.get("X-VDB-Backend", self._backend_hint)
        return self._normaliser.normalise_search(backend, resp.json())

    async def upsert_records(self, collection: str, records: list[Record]) -> list[str]:
        """Handle upsert records."""
        payload = {
            "records": [
                {
                    "record_id": r.record_id,
                    "content": r.content,
                    "metadata": dict(r.metadata),
                    "lifecycle_state": r.lifecycle_state,
                }
                for r in records
            ]
        }
        try:
            resp = await self._client.post(
                f"{self._remote_url}/collections/{collection}/records:upsert",
                json=payload,
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Remote upsert failed for {collection}") from exc
        body = resp.json()
        ids = body.get("ids")
        if isinstance(ids, list):
            return [str(v) for v in ids]
        return []

    async def delete_record(self, collection: str, record_id: str) -> bool:
        """Delete record."""
        try:
            resp = await self._client.delete(f"{self._remote_url}/collections/{collection}/records/{record_id}")
            if resp.status_code == 404:
                return False
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Remote delete failed for {collection}/{record_id}") from exc
        return True

    async def close(self) -> None:
        """Handle close."""
        await self._client.aclose()
