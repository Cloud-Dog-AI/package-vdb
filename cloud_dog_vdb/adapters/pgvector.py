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
import re
import uuid
from typing import Any

import asyncpg

from cloud_dog_vdb.adapters.base import VDBAdapter
from cloud_dog_vdb.adapters.vector_utils import deterministic_vector
from cloud_dog_vdb.config.models import ProviderConfig
from cloud_dog_vdb.domain.models import CapabilityDescriptor, CollectionSpec


class PGVectorAdapter(VDBAdapter):
    """Represent p g vector adapter."""

    def __init__(self, config: ProviderConfig, *, local_mode: bool = False) -> None:
        self.config = config
        self.local_mode = local_mode
        self._local: dict[str, dict] = {}
        self._dims: dict[str, int] = {}

    async def _conn(self) -> asyncpg.Connection:
        database_uri = str(self.config.database_uri)
        return await asyncpg.connect(database_uri)

    async def initialize(self, config: dict[str, Any] | None = None) -> bool:
        """Handle initialize."""
        _ = config
        return await self.health_check()

    async def health_check(self) -> bool:
        """Handle health check."""
        if self.local_mode:
            return True
        try:
            conn = await self._conn()
            await conn.execute("SELECT 1")
            await conn.close()
            return True
        except Exception:
            return False

    async def create_collection(self, spec: CollectionSpec) -> dict:
        """Create collection."""
        if self.local_mode:
            self._local[spec.name] = {"name": spec.name, "local": True, "docs": {}}
            self._dims[spec.name] = spec.embedding_dim
            return {"name": spec.name}
        table = self._table(spec.name)
        conn = await self._conn()
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            await conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table} ("
                "id text primary key, content text not null, metadata jsonb not null default '{}'::jsonb, "
                f"embedding vector({spec.embedding_dim}) not null)"
            )
        finally:
            await conn.close()
        self._dims[spec.name] = spec.embedding_dim
        return {"name": spec.name}

    async def get_collection(self, name: str) -> dict | None:
        """Return collection."""
        if self.local_mode:
            return self._local.get(name)
        table = self._table(name)
        conn = await self._conn()
        try:
            val = await conn.fetchval("SELECT to_regclass($1)", table)
        finally:
            await conn.close()
        if val is None:
            return None
        return {"table": str(val)}

    async def delete_collection(self, name: str) -> bool:
        """Delete collection."""
        self._dims.pop(name, None)
        if self.local_mode:
            return self._local.pop(name, None) is not None
        table = self._table(name)
        conn = await self._conn()
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {table}")
        finally:
            await conn.close()
        return True

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
        table = self._table(collection)
        conn = await self._conn()
        sql = (
            f"INSERT INTO {table}(id, content, metadata, embedding) VALUES ($1, $2, $3::jsonb, $4::vector) "
            "ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content, metadata = EXCLUDED.metadata, embedding = EXCLUDED.embedding"
        )
        try:
            for i, content in enumerate(documents):
                doc_id = ids[i] if ids and i < len(ids) else uuid.uuid4().hex
                meta = metadatas[i] if metadatas and i < len(metadatas) else {}
                vector = deterministic_vector(content, dim)
                await conn.execute(sql, doc_id, content, json.dumps(meta), self._vec_sql(vector))
                out.append(doc_id)
        finally:
            await conn.close()
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
        table = self._table(collection)
        vector_sql = self._vec_sql(deterministic_vector(query or "*", dim))
        conn = await self._conn()
        rows = []
        try:
            if filter:
                rows = await conn.fetch(
                    f"SELECT id, content, metadata, 1 - (embedding <=> $1::vector) AS score "
                    f"FROM {table} WHERE metadata @> $2::jsonb ORDER BY embedding <=> $1::vector LIMIT $3",
                    vector_sql,
                    json.dumps(filter),
                    n_results,
                )
            else:
                rows = await conn.fetch(
                    f"SELECT id, content, metadata, 1 - (embedding <=> $1::vector) AS score "
                    f"FROM {table} ORDER BY embedding <=> $1::vector LIMIT $2",
                    vector_sql,
                    n_results,
                )
        finally:
            await conn.close()
        return [
            {
                "id": str(row["id"]),
                "score": float(row["score"]),
                "content": str(row["content"]),
                "metadata": self._metadata_obj(row["metadata"]),
            }
            for row in rows
        ]

    async def delete_document(self, collection: str, doc_id: str) -> bool:
        """Delete document."""
        if self.local_mode:
            docs_state = (self._local.get(collection) or {}).get("docs", {})
            return docs_state.pop(doc_id, None) is not None
        table = self._table(collection)
        conn = await self._conn()
        try:
            result = await conn.execute(f"DELETE FROM {table} WHERE id = $1", doc_id)
        finally:
            await conn.close()
        return result.endswith("1")

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
        table = self._table(collection)
        conn = await self._conn()
        try:
            result = await conn.execute(
                f"UPDATE {table} SET content = $2, metadata = $3::jsonb, embedding = $4::vector WHERE id = $1",
                doc_id,
                content,
                json.dumps(metadata or {}),
                self._vec_sql(deterministic_vector(content, dim)),
            )
        finally:
            await conn.close()
        return result.endswith("1")

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
        table = self._table(collection)
        conn = await self._conn()
        try:
            if filter:
                out = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {table} WHERE metadata @> $1::jsonb", json.dumps(filter)
                )
            else:
                out = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        finally:
            await conn.close()
        return int(out or 0)

    def capabilities(self) -> CapabilityDescriptor:
        """Handle capabilities."""
        return CapabilityDescriptor(provider_id="pgvector", filtering=True, hybrid_search=False, delete_by_filter=True)

    @staticmethod
    def _table(name: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Invalid collection/table name: {name}")
        return name

    @staticmethod
    def _vec_sql(vector: list[float]) -> str:
        return "[" + ",".join(f"{v:.8f}" for v in vector) + "]"

    @staticmethod
    def _metadata_obj(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                loaded = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return loaded if isinstance(loaded, dict) else {}
        return {}
