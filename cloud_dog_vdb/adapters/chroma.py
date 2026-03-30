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


class ChromaAdapter(VDBAdapter):
    """Represent chroma adapter."""
    def __init__(self, config: ProviderConfig, *, local_mode: bool = False) -> None:
        self.config = config
        self.local_mode = local_mode
        self._client = httpx.AsyncClient(timeout=config.timeout_seconds)
        self._local: dict[str, dict] = {}
        self._dims: dict[str, int] = {}

    async def initialize(self, config: dict[str, Any] | None = None) -> bool:
        """Handle initialize."""
        _ = config
        return await self.health_check()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        api_key = str(self.config.api_key)
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    def _root(self) -> str:
        base_url = str(self.config.base_url)
        return f"{base_url.rstrip('/')}/api/v2/tenants/default_tenant/databases/default_database/collections"

    async def health_check(self) -> bool:
        """Handle health check."""
        if self.local_mode:
            return True
        try:
            resp = await self._client.get(
                f"{str(self.config.base_url).rstrip('/')}/api/v2/heartbeat",
                headers=self._headers(),
            )
            return resp.status_code == 200
        except Exception:
            return False

    async def create_collection(self, spec: CollectionSpec) -> dict:
        """Create collection."""
        if self.local_mode:
            data = {"name": spec.name, "id": spec.name, "local": True, "metadata": dict(spec.metadata)}
            self._local[spec.name] = data
            self._dims[spec.name] = spec.embedding_dim
            return data
        payload = {"name": spec.name, "configuration": {"hnsw": {"space": spec.distance_metric.value}}}
        if spec.metadata:
            payload["metadata"] = spec.metadata
        resp = await self._client.post(self._root(), headers=self._headers(), json=payload)
        if resp.status_code == 409:
            return {"name": spec.name, "status": "exists"}
        resp.raise_for_status()
        self._dims[spec.name] = spec.embedding_dim
        return resp.json()

    async def get_collection(self, name: str) -> dict | None:
        """Return collection."""
        if self.local_mode:
            return self._local.get(name)
        resp = await self._client.get(self._root(), headers=self._headers())
        resp.raise_for_status()
        for col in resp.json() or []:
            if col.get("name") == name:
                return col
        return None

    async def delete_collection(self, name: str) -> bool:
        """Delete collection."""
        self._dims.pop(name, None)
        if self.local_mode:
            return self._local.pop(name, None) is not None
        # Newer Chroma deployments accept collection name in delete path,
        # while some older deployments may still rely on collection ID.
        resp = await self._client.delete(f"{self._root()}/{name}", headers=self._headers())
        if resp.status_code < 300 or resp.status_code == 404:
            return True

        cid = await self._collection_id(name)
        if not cid:
            return True
        resp = await self._client.delete(f"{self._root()}/{cid}", headers=self._headers())
        return resp.status_code < 300 or resp.status_code == 404

    async def add_documents(
        self,
        collection: str,
        documents: list[str],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
    ) -> list[str]:
        """Handle add documents."""
        out: list[str] = []
        dim = self._dims.get(collection, 1024)
        docs: list[str] = []
        ids_out: list[str] = []
        metas_out: list[dict[str, Any]] = []
        embeddings: list[list[float]] = []
        for i, content in enumerate(documents):
            doc_id = ids[i] if ids and i < len(ids) else uuid.uuid4().hex
            meta = metadatas[i] if metadatas and i < len(metadatas) else {}
            docs.append(content)
            ids_out.append(doc_id)
            metas_out.append(meta)
            embeddings.append(deterministic_vector(content, dim))
            out.append(doc_id)
        if self.local_mode:
            local = self._local.setdefault(collection, {"docs": {}})
            docs_state = local.setdefault("docs", {})
            for idx, doc_id in enumerate(ids_out):
                docs_state[doc_id] = {"content": docs[idx], "metadata": metas_out[idx], "embedding": embeddings[idx]}
            return out
        cid = await self._collection_id(collection)
        if not cid:
            raise ValueError(f"Collection not found: {collection}")
        payload = {"ids": ids_out, "documents": docs, "metadatas": metas_out, "embeddings": embeddings}
        resp = await self._client.post(f"{self._root()}/{cid}/upsert", headers=self._headers(), json=payload)
        if resp.status_code == 404:
            resp = await self._client.post(f"{self._root()}/{cid}/add", headers=self._headers(), json=payload)
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
        cid = await self._collection_id(collection)
        if not cid:
            return []
        payload: dict[str, Any] = {
            "query_embeddings": [deterministic_vector(query or "*", dim)],
            "n_results": n_results,
            "include": ["distances", "metadatas", "documents"],
        }
        if filter:
            payload["where"] = self._build_where(filter)
        resp = await self._client.post(f"{self._root()}/{cid}/query", headers=self._headers(), json=payload)
        resp.raise_for_status()
        body = resp.json()
        ids = (body.get("ids") or [[]])[0]
        documents = (body.get("documents") or [[]])[0]
        metadatas = (body.get("metadatas") or [[]])[0]
        distances = (body.get("distances") or [[]])[0]
        out = []
        for i, doc_id in enumerate(ids):
            dist = float(distances[i]) if i < len(distances) else 1.0
            out.append(
                {
                    "id": str(doc_id),
                    "score": 1.0 - dist,
                    "content": documents[i] if i < len(documents) else "",
                    "metadata": metadatas[i] if i < len(metadatas) else {},
                }
            )
        return out

    async def delete_document(self, collection: str, doc_id: str) -> bool:
        """Delete document."""
        if self.local_mode:
            docs_state = (self._local.get(collection) or {}).get("docs", {})
            return docs_state.pop(doc_id, None) is not None
        cid = await self._collection_id(collection)
        if not cid:
            return False
        resp = await self._client.post(
            f"{self._root()}/{cid}/delete",
            headers=self._headers(),
            json={"ids": [doc_id]},
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
        cid = await self._collection_id(collection)
        if not cid:
            return False
        payload = {
            "ids": [doc_id],
            "documents": [content],
            "metadatas": [metadata or {}],
            "embeddings": [deterministic_vector(content, dim)],
        }
        resp = await self._client.post(f"{self._root()}/{cid}/upsert", headers=self._headers(), json=payload)
        if resp.status_code == 404:
            resp = await self._client.post(f"{self._root()}/{cid}/update", headers=self._headers(), json=payload)
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
        cid = await self._collection_id(collection)
        if not cid:
            return 0
        if filter:
            docs = await self.search(collection, "*", 10000, filter, {})
            return len(docs)
        resp = await self._client.get(f"{self._root()}/{cid}/count", headers=self._headers())
        if resp.status_code == 404:
            return 0
        resp.raise_for_status()
        body = resp.json()
        return int(body if isinstance(body, int) else body.get("count", 0))

    def capabilities(self) -> CapabilityDescriptor:
        """Handle capabilities."""
        return CapabilityDescriptor(provider_id="chroma", filtering=True, hybrid_search=False, delete_by_filter=False)

    async def _collection_id(self, name: str) -> str | None:
        resp = await self._client.get(self._root(), headers=self._headers())
        resp.raise_for_status()
        for col in resp.json() or []:
            if col.get("name") == name:
                return str(col.get("id") or col.get("name"))
        return None

    @staticmethod
    def _build_where(filter: dict[str, Any]) -> dict[str, Any]:
        if len(filter) <= 1:
            return dict(filter)
        return {"$and": [{k: v} for k, v in filter.items()]}
