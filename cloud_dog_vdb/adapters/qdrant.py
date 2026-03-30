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

from __future__ import annotations

import uuid
from typing import Any

import httpx

from cloud_dog_vdb.adapters.base import VDBAdapter
from cloud_dog_vdb.adapters.vector_utils import deterministic_vector
from cloud_dog_vdb.config.models import ProviderConfig
from cloud_dog_vdb.domain.models import CapabilityDescriptor, CollectionSpec


class QdrantAdapter(VDBAdapter):
    """Represent qdrant adapter."""
    def __init__(self, config: ProviderConfig, *, local_mode: bool = False) -> None:
        self.config = config
        self.local_mode = local_mode
        self._client = httpx.AsyncClient(timeout=config.timeout_seconds)
        self._local: dict[str, dict] = {}
        self._dims: dict[str, int] = {}

    def _base(self) -> str:
        return str(self.config.base_url).rstrip("/")

    async def initialize(self, config: dict[str, Any] | None = None) -> bool:
        """Handle initialize."""
        _ = config
        return await self.health_check()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        api_key = str(self.config.api_key)
        if api_key:
            headers["api-key"] = api_key
        return headers

    async def health_check(self) -> bool:
        """Handle health check."""
        if self.local_mode:
            return True
        try:
            resp = await self._client.get(f"{self._base()}/collections", headers=self._headers())
            return resp.status_code == 200
        except Exception:
            return False

    async def create_collection(self, spec: CollectionSpec) -> dict:
        """Create collection."""
        if self.local_mode:
            self._local[spec.name] = {"name": spec.name, "local": True, "docs": {}}
            self._dims[spec.name] = spec.embedding_dim
            return {"name": spec.name, "status": "created"}
        payload = {"vectors": {"size": spec.embedding_dim, "distance": spec.distance_metric.value.capitalize()}}
        resp = await self._client.put(
            f"{self._base()}/collections/{spec.name}",
            headers=self._headers(),
            json=payload,
        )
        resp.raise_for_status()
        self._dims[spec.name] = spec.embedding_dim
        return {"name": spec.name, "status": "created"}

    async def get_collection(self, name: str) -> dict | None:
        """Return collection."""
        if self.local_mode:
            return self._local.get(name)
        resp = await self._client.get(f"{self._base()}/collections/{name}", headers=self._headers())
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    async def delete_collection(self, name: str) -> bool:
        """Delete collection."""
        self._dims.pop(name, None)
        if self.local_mode:
            return self._local.pop(name, None) is not None
        resp = await self._client.delete(f"{self._base()}/collections/{name}", headers=self._headers())
        return resp.status_code < 300

    async def add_documents(
        self,
        collection: str,
        documents: list[str],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
    ) -> list[str]:
        """Handle add documents."""
        dim = self._dims.get(collection, 1024)
        if self.local_mode:
            local = self._local.setdefault(collection, {"docs": {}})
            docs_state = local.setdefault("docs", {})
            out: list[str] = []
            for i, content in enumerate(documents):
                eid = ids[i] if ids and i < len(ids) else uuid.uuid4().hex
                meta = metadatas[i] if metadatas and i < len(metadatas) else {}
                docs_state[eid] = {
                    "content": content,
                    "metadata": meta,
                    "embedding": deterministic_vector(content, dim),
                }
                out.append(eid)
            return out
        points: list[dict[str, Any]] = []
        out = []
        for i, content in enumerate(documents):
            external_id = ids[i] if ids and i < len(ids) else uuid.uuid4().hex
            doc_id = self._point_id(external_id)
            meta = metadatas[i] if metadatas and i < len(metadatas) else {}
            payload = {"text": content, "metadata": meta, "external_id": external_id}
            points.append({"id": doc_id, "vector": deterministic_vector(content, dim), "payload": payload})
            out.append(external_id)
        resp = await self._client.put(
            f"{self._base()}/collections/{collection}/points?wait=true",
            headers=self._headers(),
            json={"points": points},
        )
        resp.raise_for_status()
        return out

    async def search(
        self,
        collection: str,
        query: str,
        n_results: int,
        filter: dict[str, Any] | None = None,
        search_options: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Handle search."""
        _ = search_options
        dim = self._dims.get(collection, 1024)
        if self.local_mode:
            docs_state = (self._local.get(collection) or {}).get("docs", {})
            target = deterministic_vector(query or "*", dim)
            out: list[dict[str, Any]] = []
            for doc_id, value in docs_state.items():
                metadata = value.get("metadata") or {}
                if filter and any(metadata.get(k) != v for k, v in filter.items()):
                    continue
                emb = value.get("embedding") or target
                score = sum(a * b for a, b in zip(emb, target))
                out.append(
                    {
                        "id": doc_id,
                        "score": float(score),
                        "content": value.get("content", ""),
                        "metadata": metadata,
                    }
                )
            out.sort(key=lambda x: x["score"], reverse=True)
            return out[:n_results]
        body: dict[str, Any] = {
            "vector": deterministic_vector(query or "*", dim),
            "limit": n_results,
            "with_payload": True,
        }
        if filter:
            body["filter"] = {"must": [{"key": f"metadata.{k}", "match": {"value": v}} for k, v in filter.items()]}
        resp = await self._client.post(
            f"{self._base()}/collections/{collection}/points/search",
            headers=self._headers(),
            json=body,
        )
        resp.raise_for_status()
        raw = resp.json().get("result") or []
        if isinstance(raw, dict):
            raw = raw.get("points") or []
        out = []
        for item in raw:
            payload = item.get("payload") or {}
            out.append(
                {
                    "id": str(payload.get("external_id") or item.get("id")),
                    "score": float(item.get("score", 0.0)),
                    "content": payload.get("text", ""),
                    "metadata": payload.get("metadata", {}),
                }
            )
        return out

    async def delete_document(self, collection: str, doc_id: str) -> bool:
        """Delete document."""
        if self.local_mode:
            docs_state = (self._local.get(collection) or {}).get("docs", {})
            return docs_state.pop(doc_id, None) is not None
        point_id = self._point_id(doc_id)
        resp = await self._client.post(
            f"{self._base()}/collections/{collection}/points/delete?wait=true",
            headers=self._headers(),
            json={"points": [point_id]},
        )
        return resp.status_code < 300

    async def update_document(
        self, collection: str, doc_id: str, content: str, metadata: dict[str, Any] | None = None
    ) -> bool:
        """Update document."""
        dim = self._dims.get(collection, 1024)
        if self.local_mode:
            docs_state = (self._local.get(collection) or {}).setdefault("docs", {})
            if doc_id not in docs_state:
                return False
            docs_state[doc_id] = {
                "content": content,
                "metadata": metadata or {},
                "embedding": deterministic_vector(content, dim),
            }
            return True
        point_id = self._point_id(doc_id)
        payload = {"text": content, "metadata": metadata or {}}
        resp = await self._client.put(
            f"{self._base()}/collections/{collection}/points?wait=true",
            headers=self._headers(),
            json={
                "points": [
                    {
                        "id": point_id,
                        "vector": deterministic_vector(content, dim),
                        "payload": {**payload, "external_id": doc_id},
                    }
                ]
            },
        )
        return resp.status_code < 300

    async def count_documents(self, collection: str, filter: dict[str, Any] | None = None) -> int:
        """Handle count documents."""
        if self.local_mode:
            docs_state = (self._local.get(collection) or {}).get("docs", {})
            if not filter:
                return len(docs_state)
            return sum(
                1
                for value in docs_state.values()
                if all((value.get("metadata") or {}).get(k) == v for k, v in filter.items())
            )
        payload: dict[str, Any] = {"exact": True}
        if filter:
            payload["filter"] = {"must": [{"key": f"metadata.{k}", "match": {"value": v}} for k, v in filter.items()]}
        resp = await self._client.post(
            f"{self._base()}/collections/{collection}/points/count",
            headers=self._headers(),
            json=payload,
        )
        resp.raise_for_status()
        result = resp.json().get("result") or {}
        return int(result.get("count", 0))

    def capabilities(self) -> CapabilityDescriptor:
        """Handle capabilities."""
        return CapabilityDescriptor(provider_id="qdrant", filtering=True, hybrid_search=False, delete_by_filter=True)

    @staticmethod
    def _point_id(external_id: str) -> str:
        try:
            return str(uuid.UUID(external_id))
        except ValueError:
            return str(uuid.uuid5(uuid.NAMESPACE_URL, f"qdrant:{external_id}"))
