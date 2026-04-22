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


class InfinityAdapter(VDBAdapter):
    """Represent infinity adapter."""

    _STATIC_METADATA_COLUMNS = (
        "tenant_id",
        "source_uri",
        "source_type",
        "lifecycle_state",
        "created_at",
        "doc_id",
        "chunk_id",
        "content_hash",
        "embedding_model",
        "chunker_version",
        "is_latest",
    )

    def __init__(self, config: ProviderConfig, *, local_mode: bool = False) -> None:
        self.config = config
        self.local_mode = local_mode
        self._client = httpx.AsyncClient(timeout=config.timeout_seconds)
        self._local: dict[str, dict[str, Any]] = {}
        self._dims: dict[str, int] = {}
        self._meta: dict[str, dict[str, dict[str, Any]]] = {}

    def _database(self) -> str:
        return str(self.config.database or "default_db")

    def _base(self) -> str:
        if self.config.base_url:
            return self.config.base_url.rstrip("/")
        host = str(self.config.host)
        port = int(self.config.port or 8080)
        scheme = "https" if port == 443 else "http"
        return f"{scheme}://{host}:{port}"

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        return headers

    async def initialize(self, config: dict[str, Any] | None = None) -> bool:
        """Handle initialize."""
        _ = config
        return await self.health_check()

    async def health_check(self) -> bool:
        """Handle health check."""
        if self.local_mode:
            return True
        try:
            resp = await self._client.get(f"{self._base()}/databases", headers=self._headers())
            return resp.status_code == 200
        except Exception:
            return False

    async def create_collection(self, spec: CollectionSpec) -> dict:
        """Create collection."""
        self._dims[spec.name] = spec.embedding_dim
        self._meta.setdefault(spec.name, {})
        if self.local_mode:
            self._local.setdefault(spec.name, {})
            return {"name": spec.name, "status": "created"}

        fields = [
            {"name": "record_id", "type": "varchar"},
            {"name": "content", "type": "varchar"},
            {"name": "metadata_json", "type": "varchar"},
            {"name": "dense_vector", "type": f"vector,{spec.embedding_dim},float"},
        ]
        fields.extend({"name": name, "type": "varchar"} for name in self._STATIC_METADATA_COLUMNS)
        payload = {"create_option": "ignore_if_exists", "fields": fields}
        resp = await self._client.post(
            f"{self._base()}/databases/{self._database()}/tables/{spec.name}",
            headers=self._headers(),
            json=payload,
        )
        resp.raise_for_status()
        out = resp.json()
        if int(out.get("error_code", 0)) != 0:
            raise ValueError(f"infinity create_collection failed: {out}")
        return {"name": spec.name, "status": "created"}

    async def get_collection(self, name: str) -> dict | None:
        """Return collection."""
        if self.local_mode:
            return {"name": name} if name in self._local else None
        resp = await self._client.get(
            f"{self._base()}/databases/{self._database()}/tables/{name}",
            headers=self._headers(),
        )
        if resp.status_code == 404:
            return None
        if resp.status_code >= 400:
            return None
        out = resp.json()
        if int(out.get("error_code", 0)) != 0:
            return None
        return out

    async def delete_collection(self, name: str) -> bool:
        """Delete collection."""
        self._dims.pop(name, None)
        self._meta.pop(name, None)
        if self.local_mode:
            return self._local.pop(name, None) is not None
        resp = await self._client.request(
            "DELETE",
            f"{self._base()}/databases/{self._database()}/tables/{name}",
            headers=self._headers(),
            json={"drop_option": "ignore_if_not_exists"},
        )
        if resp.status_code == 404:
            return True
        if resp.status_code >= 400:
            return False
        out = resp.json()
        return int(out.get("error_code", 0)) == 0

    async def add_documents(
        self,
        collection: str,
        documents: list[str],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
    ) -> list[str]:
        """Handle add documents."""
        dim = self._dims.get(collection, 1024)
        rows: list[dict[str, Any]] = []
        out: list[str] = []
        self._meta.setdefault(collection, {})

        for i, content in enumerate(documents):
            record_id = ids[i] if ids and i < len(ids) else uuid.uuid4().hex
            metadata = dict(metadatas[i] if metadatas and i < len(metadatas) else {})
            metadata.setdefault("record_id", record_id)
            vector = deterministic_vector(content, dim)
            row = {
                "record_id": record_id,
                "content": content,
                "metadata_json": json.dumps(metadata, sort_keys=True, default=str),
                "dense_vector": vector,
            }
            for key in self._STATIC_METADATA_COLUMNS:
                value = metadata.get(key, "")
                row[key] = str(value)
            rows.append(row)
            out.append(record_id)
            self._meta[collection][record_id] = metadata

        if self.local_mode:
            store = self._local.setdefault(collection, {})
            for row in rows:
                store[str(row["record_id"])] = row
            return out

        resp = await self._client.post(
            f"{self._base()}/databases/{self._database()}/tables/{collection}/docs",
            headers=self._headers(),
            json=rows,
        )
        resp.raise_for_status()
        body = resp.json()
        if int(body.get("error_code", 0)) != 0:
            raise ValueError(f"infinity add_documents failed: {body}")
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
        filters = dict(filter or {})
        dim = self._dims.get(collection, 1024)

        if self.local_mode:
            target = deterministic_vector(query or "*", dim)
            out: list[dict[str, Any]] = []
            for doc_id, row in self._local.get(collection, {}).items():
                metadata = self._metadata_for_row(collection, row)
                if any(metadata.get(k) != v for k, v in filters.items()):
                    continue
                score = sum(a * b for a, b in zip(row.get("dense_vector", []), target))
                out.append(
                    {
                        "id": doc_id,
                        "score": float(score),
                        "content": row.get("content", ""),
                        "metadata": metadata,
                    }
                )
            out.sort(key=lambda item: item["score"], reverse=True)
            return out[:n_results]

        expression = self._filter_expression(filters)
        payload: dict[str, Any] = {
            "output": ["record_id", "content", "metadata_json"],
            "filter": expression,
            "search": [
                {
                    "match_method": "dense",
                    "fields": "dense_vector",
                    "query_vector": deterministic_vector(query or "*", dim),
                    "element_type": "float",
                    "metric_type": str(search_options.get("metric_type", "ip")),
                    "topn": int(max(n_results, 1)),
                }
            ],
        }
        resp = await self._client.request(
            "GET",
            f"{self._base()}/databases/{self._database()}/tables/{collection}/docs",
            headers=self._headers(),
            json=payload,
        )
        resp.raise_for_status()
        body = resp.json()
        if int(body.get("error_code", 0)) != 0:
            raise ValueError(f"infinity search failed: {body}")

        rows = self._rows_from_output(body.get("output"))
        out: list[dict[str, Any]] = []
        for index, row in enumerate(rows):
            record_id = str(row.get("record_id", ""))
            if not record_id:
                continue
            metadata = self._metadata_for_row(collection, row)
            if any(metadata.get(key) != value for key, value in filters.items()):
                continue
            out.append(
                {
                    "id": record_id,
                    "score": float(max(n_results - index, 1)),
                    "content": str(row.get("content", "")),
                    "metadata": metadata,
                }
            )
        return out[:n_results]

    async def delete_document(self, collection: str, doc_id: str) -> bool:
        """Delete document."""
        if self.local_mode:
            return self._local.get(collection, {}).pop(doc_id, None) is not None

        expression = f"record_id = {self._quote(doc_id)}"
        resp = await self._client.request(
            "DELETE",
            f"{self._base()}/databases/{self._database()}/tables/{collection}/docs",
            headers=self._headers(),
            json={"filter": expression},
        )
        if resp.status_code >= 400:
            return False
        body = resp.json()
        deleted = int(body.get("deleted_rows", 0))
        return int(body.get("error_code", 0)) == 0 and deleted >= 1

    async def update_document(
        self,
        collection: str,
        doc_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Update document."""
        existing = await self.delete_document(collection, doc_id)
        if not existing:
            return False
        out = await self.add_documents(collection, [content], [metadata or {}], [doc_id])
        return len(out) == 1

    async def count_documents(self, collection: str, filter: dict[str, Any] | None = None) -> int:
        """Handle count documents."""
        filters = dict(filter or {})
        if self.local_mode:
            return len(await self.search(collection, "*", 10_000, filters, {}))

        expression = self._filter_expression(filters)
        resp = await self._client.request(
            "GET",
            f"{self._base()}/databases/{self._database()}/tables/{collection}/docs",
            headers=self._headers(),
            json={"output": ["record_id", "metadata_json"], "filter": expression},
        )
        resp.raise_for_status()
        body = resp.json()
        if int(body.get("error_code", 0)) != 0:
            raise ValueError(f"infinity count_documents failed: {body}")
        rows = self._rows_from_output(body.get("output"))
        if not filters:
            return len(rows)
        count = 0
        for row in rows:
            metadata = self._metadata_for_row(collection, row)
            if all(metadata.get(key) == value for key, value in filters.items()):
                count += 1
        return count

    def capabilities(self) -> CapabilityDescriptor:
        """Handle capabilities."""
        return CapabilityDescriptor(
            provider_id="infinity",
            filtering=True,
            hybrid_search=False,
            sparse_vectors=False,
            multi_vector=False,
            metadata_indexing=True,
            upsert_semantics=True,
            delete_by_filter=True,
            ttl_native=False,
            transactions=False,
            consistency=False,
            max_metadata_bytes=65536,
            max_batch_size=500,
            supports_multimodal=False,
        )

    @staticmethod
    def _quote(value: Any) -> str:
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, bool):
            return "true" if value else "false"
        text = str(value).replace("'", "''")
        return f"'{text}'"

    def _filter_expression(self, filters: dict[str, Any]) -> str:
        if not filters:
            return "record_id = record_id"
        clauses: list[str] = []
        for key, value in filters.items():
            if key == "id":
                key = "record_id"
            if key in self._STATIC_METADATA_COLUMNS or key in {"record_id"}:
                clauses.append(f"{key} = {self._quote(value)}")
        if not clauses:
            return "record_id = record_id"
        return " and ".join(clauses)

    @staticmethod
    def _rows_from_output(raw_output: Any) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if not isinstance(raw_output, list):
            return rows
        for row in raw_output:
            if isinstance(row, dict):
                rows.append(dict(row))
                continue
            if not isinstance(row, list):
                continue
            row_dict: dict[str, Any] = {}
            for item in row:
                if isinstance(item, dict):
                    row_dict.update(item)
            rows.append(row_dict)
        return rows

    def _metadata_for_row(self, collection: str, row: dict[str, Any]) -> dict[str, Any]:
        metadata_raw = row.get("metadata_json", "")
        if isinstance(metadata_raw, str) and metadata_raw:
            try:
                loaded = json.loads(metadata_raw)
                if isinstance(loaded, dict):
                    return loaded
            except json.JSONDecodeError:
                pass
        record_id = str(row.get("record_id", ""))
        if record_id:
            return dict(self._meta.get(collection, {}).get(record_id, {}))
        return {}
