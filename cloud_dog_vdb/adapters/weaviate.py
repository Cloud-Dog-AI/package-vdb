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


class WeaviateAdapter(VDBAdapter):
    """Represent weaviate adapter."""

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
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    async def health_check(self) -> bool:
        """Handle health check."""
        if self.local_mode:
            return True
        try:
            resp = await self._client.get(f"{self._base()}/v1/meta", headers=self._headers())
            return resp.status_code == 200
        except Exception:
            return False

    @staticmethod
    def _class_name(name: str) -> str:
        parts = [p for p in name.replace("-", "_").split("_") if p]
        out = "".join(p[:1].upper() + p[1:] for p in parts)
        return out or "CloudDogAiVdb"

    async def create_collection(self, spec: CollectionSpec) -> dict:
        """Create collection."""
        class_name = self._class_name(spec.name)
        if self.local_mode:
            self._local[spec.name] = {"name": spec.name, "class": class_name, "local": True, "docs": {}}
            self._dims[spec.name] = spec.embedding_dim
            return {"name": spec.name, "class": class_name}
        payload = {
            "class": class_name,
            "description": spec.name,
            "vectorizer": "none",
            "properties": [
                {"name": "text", "dataType": ["text"]},
                {"name": "external_id", "dataType": ["text"]},
                {"name": "metadata_json", "dataType": ["text"]},
            ],
        }
        resp = await self._client.post(f"{self._base()}/v1/schema", headers=self._headers(), json=payload)
        if resp.status_code not in {200, 201, 422}:
            resp.raise_for_status()
        self._dims[spec.name] = spec.embedding_dim
        return {"name": spec.name, "class": class_name}

    async def get_collection(self, name: str) -> dict | None:
        """Return collection."""
        if self.local_mode:
            return self._local.get(name)
        class_name = self._class_name(name)
        resp = await self._client.get(f"{self._base()}/v1/schema/{class_name}", headers=self._headers())
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    async def delete_collection(self, name: str) -> bool:
        """Delete collection."""
        self._dims.pop(name, None)
        if self.local_mode:
            return self._local.pop(name, None) is not None
        class_name = self._class_name(name)
        resp = await self._client.delete(f"{self._base()}/v1/schema/{class_name}", headers=self._headers())
        return resp.status_code < 300 or resp.status_code == 404

    async def add_documents(
        self,
        collection: str,
        documents: list[str],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
    ) -> list[str]:
        """Handle add documents."""
        class_name = self._class_name(collection)
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
        for i, content in enumerate(documents):
            external_id = ids[i] if ids and i < len(ids) else uuid.uuid4().hex
            doc_id = self._object_uuid(collection, external_id)
            meta = metadatas[i] if metadatas and i < len(metadatas) else {}
            payload = {
                "class": class_name,
                "id": doc_id,
                "properties": {"text": content, "external_id": external_id, "metadata_json": json.dumps(meta)},
                "vector": deterministic_vector(content, dim),
            }
            resp = await self._client.post(
                f"{self._base()}/v1/objects",
                headers=self._headers(),
                json=payload,
            )
            resp.raise_for_status()
            out.append(external_id)
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
        class_name = self._class_name(collection)
        vector = ",".join(f"{v:.8f}" for v in deterministic_vector(query or "*", dim))
        gql = (
            "{Get{"
            + class_name
            + f"(nearVector:{{vector:[{vector}]}} limit:{n_results})"
            + "{text external_id metadata_json _additional{id distance}}}}"
        )
        resp = await self._client.post(
            f"{self._base()}/v1/graphql",
            headers=self._headers(),
            json={"query": gql},
        )
        resp.raise_for_status()
        items = ((resp.json().get("data") or {}).get("Get") or {}).get(class_name) or []
        out: list[dict[str, Any]] = []
        for item in items:
            metadata_raw = item.get("metadata_json", "{}")
            try:
                metadata = json.loads(metadata_raw) if isinstance(metadata_raw, str) else {}
            except json.JSONDecodeError:
                metadata = {}
            if filter and any(metadata.get(k) != v for k, v in filter.items()):
                continue
            additional = item.get("_additional") or {}
            distance = float(additional.get("distance", 1.0))
            out.append(
                {
                    "id": item.get("external_id") or additional.get("id"),
                    "score": 1.0 - distance,
                    "content": item.get("text", ""),
                    "metadata": metadata,
                }
            )
        return out[:n_results]

    async def delete_document(self, collection: str, doc_id: str) -> bool:
        """Delete document."""
        if self.local_mode:
            docs_state = (self._local.get(collection) or {}).get("docs", {})
            return docs_state.pop(doc_id, None) is not None
        object_id = self._object_uuid(collection, doc_id)
        resp = await self._client.delete(f"{self._base()}/v1/objects/{object_id}", headers=self._headers())
        return resp.status_code < 300 or resp.status_code == 404

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
        class_name = self._class_name(collection)
        object_id = self._object_uuid(collection, doc_id)
        body = {
            "class": class_name,
            "id": object_id,
            "properties": {"text": content, "external_id": doc_id, "metadata_json": json.dumps(metadata or {})},
            "vector": deterministic_vector(content, dim),
        }
        resp = await self._client.put(f"{self._base()}/v1/objects/{object_id}", headers=self._headers(), json=body)
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
        class_name = self._class_name(collection)
        gql = f"{{Aggregate{{{class_name}{{meta{{count}}}}}}}}"
        resp = await self._client.post(
            f"{self._base()}/v1/graphql",
            headers=self._headers(),
            json={"query": gql},
        )
        resp.raise_for_status()
        rows = ((resp.json().get("data") or {}).get("Aggregate") or {}).get(class_name) or []
        count = int(((rows[0] if rows else {}).get("meta") or {}).get("count", 0))
        if not filter:
            return count
        # Filtered counts are emulated by reading candidates from search endpoint.
        return len(await self.search(collection, "*", max(count, 1), filter, {}))

    def capabilities(self) -> CapabilityDescriptor:
        """Handle capabilities."""
        return CapabilityDescriptor(provider_id="weaviate", filtering=True, hybrid_search=True, delete_by_filter=False)

    @staticmethod
    def _object_uuid(collection: str, external_id: str) -> str:
        seed = f"{collection}:{external_id}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))
