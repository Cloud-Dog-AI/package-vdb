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

import json
import uuid
from typing import Any

import httpx

from cloud_dog_vdb.adapters.base import VDBAdapter
from cloud_dog_vdb.adapters.vector_utils import deterministic_vector
from cloud_dog_vdb.config.models import ProviderConfig
from cloud_dog_vdb.domain.models import CapabilityDescriptor, CollectionSpec


class OpenSearchAdapter(VDBAdapter):
    """Represent open search adapter."""
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

    def _auth(self) -> tuple[str, str] | None:
        username = str(self.config.username)
        password = str(self.config.password)
        if username:
            return (username, password)
        return None

    def _base(self) -> str:
        runtime_base = str(self.config.base_url)
        if runtime_base:
            return runtime_base.rstrip("/")
        port = int(self.config.port or 0)
        host = str(self.config.host)
        scheme = "https" if port == 443 else "http"
        return f"{scheme}://{host}:{port}"

    async def health_check(self) -> bool:
        """Handle health check."""
        if self.local_mode:
            return True
        try:
            resp = await self._client.get(f"{self._base()}/", auth=self._auth())
            return resp.status_code == 200
        except Exception:
            return False

    async def create_collection(self, spec: CollectionSpec) -> dict:
        """Create collection."""
        if self.local_mode:
            self._local[spec.name] = {"name": spec.name, "local": True, "docs": {}}
            self._dims[spec.name] = spec.embedding_dim
            return {"name": spec.name}
        payload = {
            "settings": {"index": {"knn": True}},
            "mappings": {
                "properties": {
                    "vector": {"type": "knn_vector", "dimension": spec.embedding_dim},
                    "text": {"type": "text"},
                    "metadata": {"type": "object", "enabled": True},
                }
            },
        }
        resp = await self._client.put(f"{self._base()}/{spec.name}", auth=self._auth(), json=payload)
        resp.raise_for_status()
        self._dims[spec.name] = spec.embedding_dim
        return {"name": spec.name}

    async def get_collection(self, name: str) -> dict | None:
        """Return collection."""
        if self.local_mode:
            return self._local.get(name)
        resp = await self._client.get(f"{self._base()}/{name}", auth=self._auth())
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    async def delete_collection(self, name: str) -> bool:
        """Delete collection."""
        self._dims.pop(name, None)
        if self.local_mode:
            return self._local.pop(name, None) is not None
        resp = await self._client.delete(f"{self._base()}/{name}", auth=self._auth())
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
        out: list[str] = []
        if self.local_mode:
            local = self._local.setdefault(collection, {"docs": {}})
            docs_state = local.setdefault("docs", {})
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
        lines: list[str] = []
        for i, content in enumerate(documents):
            doc_id = ids[i] if ids and i < len(ids) else uuid.uuid4().hex
            meta = metadatas[i] if metadatas and i < len(metadatas) else {}
            lines.append(json.dumps({"index": {"_index": collection, "_id": doc_id}}))
            lines.append(
                json.dumps(
                    {
                        "text": content,
                        "metadata": meta,
                        "vector": deterministic_vector(content, dim),
                    }
                )
            )
            out.append(doc_id)
        payload = "\n".join(lines) + "\n"
        resp = await self._client.post(
            f"{self._base()}/_bulk?refresh=true",
            auth=self._auth(),
            headers={"Content-Type": "application/x-ndjson"},
            content=payload,
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
        search_options = search_options or {}
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
        vector = deterministic_vector(query or "*", dim)
        filters = [{"term": {f"metadata.{k}": v}} for k, v in (filter or {}).items()]
        if search_options.get("hybrid_enabled"):
            query_body: dict[str, Any] = {
                "size": n_results,
                "query": {
                    "bool": {
                        "should": [
                            {"knn": {"vector": {"vector": vector, "k": n_results}}},
                            {"match": {"text": query}},
                        ],
                        "minimum_should_match": 1,
                        "filter": filters,
                    }
                },
            }
        else:
            query_body = {
                "size": n_results,
                "query": {
                    "bool": {
                        "must": [{"knn": {"vector": {"vector": vector, "k": n_results}}}],
                        "filter": filters,
                    }
                },
            }
        resp = await self._client.post(f"{self._base()}/{collection}/_search", auth=self._auth(), json=query_body)
        resp.raise_for_status()
        hits = (resp.json().get("hits") or {}).get("hits") or []
        return [
            {
                "id": str(item.get("_id")),
                "score": float(item.get("_score", 0.0)),
                "content": (item.get("_source") or {}).get("text", ""),
                "metadata": (item.get("_source") or {}).get("metadata", {}),
            }
            for item in hits
        ]

    async def delete_document(self, collection: str, doc_id: str) -> bool:
        """Delete document."""
        if self.local_mode:
            docs_state = (self._local.get(collection) or {}).get("docs", {})
            return docs_state.pop(doc_id, None) is not None
        resp = await self._client.delete(f"{self._base()}/{collection}/_doc/{doc_id}?refresh=true", auth=self._auth())
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
        body = {
            "doc": {"text": content, "metadata": metadata or {}, "vector": deterministic_vector(content, dim)},
            "doc_as_upsert": True,
        }
        resp = await self._client.post(
            f"{self._base()}/{collection}/_update/{doc_id}?refresh=true",
            auth=self._auth(),
            json=body,
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
        body = {"query": {"match_all": {}}}
        if filter:
            body = {"query": {"bool": {"filter": [{"term": {f"metadata.{k}": v}} for k, v in filter.items()]}}}
        resp = await self._client.post(f"{self._base()}/{collection}/_count", auth=self._auth(), json=body)
        resp.raise_for_status()
        return int(resp.json().get("count", 0))

    def capabilities(self) -> CapabilityDescriptor:
        """Handle capabilities."""
        return CapabilityDescriptor(provider_id="opensearch", filtering=True, hybrid_search=True, delete_by_filter=True)
