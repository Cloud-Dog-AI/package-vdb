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

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime, timezone

from cloud_dog_vdb.adapters.registry import AdapterRegistry
from cloud_dog_vdb.domain.models import CollectionSpec, Record, SearchRequest, SearchResponse, SearchResult
from cloud_dog_vdb.lifecycle.manager import mark_deleted, mark_superseded
from cloud_dog_vdb.metadata.filters import coerce_metadata_filter, filter_to_backend_query, matches_metadata
from cloud_dog_vdb.metadata.identity import compute_content_hash, compute_doc_id, compute_record_id
from cloud_dog_vdb.metadata.provenance import merge_provenance
from cloud_dog_vdb.metadata.schema import validate_metadata
from cloud_dog_vdb.versioning.schema_version import SchemaVersionManager


class VDBClient:
    """Represent v d b client."""

    def __init__(self, registry: AdapterRegistry, default_provider_id: str) -> None:
        self._registry = registry
        self._default = default_provider_id
        self._collections: dict[str, dict[str, CollectionSpec]] = {}
        self._records: dict[str, dict[str, dict[str, Record]]] = {}
        self._schema_versions = SchemaVersionManager()

    def _adapter(self, provider_id: str | None):
        return self._registry.get(provider_id or self._default)

    def _provider(self, provider_id: str | None) -> str:
        return provider_id or self._default

    def _collection_store(self, provider_id: str) -> dict[str, CollectionSpec]:
        return self._collections.setdefault(provider_id, {})

    def _record_store(self, provider_id: str, collection: str) -> dict[str, Record]:
        backend_records = self._records.setdefault(provider_id, {})
        return backend_records.setdefault(collection, {})

    async def init_backend(self, profile: str | None = None) -> bool:
        """Handle init backend."""
        _ = profile
        return await self._adapter(None).initialize()

    async def health_check(self, provider_id: str | None = None) -> bool:
        """Handle health check."""
        return await self._adapter(provider_id).health_check()

    async def create_collection(self, spec: CollectionSpec, provider_id: str | None = None) -> dict:
        """Create collection."""
        out = await self._adapter(provider_id).create_collection(spec)
        pid = self._provider(provider_id)
        self._collection_store(pid)[spec.name] = spec
        self._record_store(pid, spec.name)
        self._schema_versions.register(
            spec.name,
            dimension_count=spec.embedding_dim,
            metadata_fields=list(spec.metadata_schema.keys()),
            embedding_model=str(spec.metadata.get("embedding_model", "unspecified")),
        )
        return out

    async def get_collection(self, name: str, provider_id: str | None = None) -> dict | None:
        """Return collection."""
        pid = self._provider(provider_id)
        local = self._collection_store(pid).get(name)
        if local is not None:
            return {
                "name": local.name,
                "namespace": local.namespace,
                "embedding_dim": local.embedding_dim,
                "distance_metric": local.distance_metric.value,
                "metadata": dict(local.metadata),
                "metadata_schema": dict(local.metadata_schema),
                "index_params": dict(local.index_params),
                "access_policy": dict(local.access_policy),
            }
        return await self._adapter(provider_id).get_collection(name)

    async def list_collections(self, provider_id: str | None = None) -> list[dict]:
        """List collections."""
        pid = self._provider(provider_id)
        return [await self.get_collection(name, pid) for name in sorted(self._collection_store(pid))]

    async def update_collection(self, name: str, patch: dict, provider_id: str | None = None) -> dict | None:
        """Update collection."""
        pid = self._provider(provider_id)
        spec = self._collection_store(pid).get(name)
        if spec is None:
            return None
        updated = replace(
            spec,
            namespace=str(patch.get("namespace", spec.namespace)),
            embedding_dim=int(patch.get("embedding_dim", spec.embedding_dim)),
            metadata={**spec.metadata, **patch.get("metadata", {})},
            metadata_schema={**spec.metadata_schema, **patch.get("metadata_schema", {})},
            index_params={**spec.index_params, **patch.get("index_params", {})},
            access_policy={**spec.access_policy, **patch.get("access_policy", {})},
        )
        self._collection_store(pid)[name] = updated
        self._schema_versions.register(
            updated.name,
            dimension_count=updated.embedding_dim,
            metadata_fields=list(updated.metadata_schema.keys()),
            embedding_model=str(updated.metadata.get("embedding_model", "unspecified")),
        )
        return await self.get_collection(name, pid)

    async def delete_collection(self, name: str, provider_id: str | None = None) -> bool:
        """Delete collection."""
        pid = self._provider(provider_id)
        self._collection_store(pid).pop(name, None)
        self._records.setdefault(pid, {}).pop(name, None)
        return await self._adapter(provider_id).delete_collection(name)

    def _normalise_record(self, collection: str, provider_id: str, record: Record) -> Record:
        clean_content = record.content.replace("\x00", "")
        metadata = dict(record.metadata)
        collection_spec = self._collection_store(provider_id).get(collection)
        if collection_spec is not None:
            metadata.setdefault("collection", collection_spec.name)
            metadata.setdefault("namespace", collection_spec.namespace)
        metadata = merge_provenance(metadata)
        metadata.setdefault("lifecycle_state", "active")
        metadata.setdefault("is_latest", True)
        metadata.setdefault("created_at", datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"))
        content_hash = str(metadata.get("content_hash") or compute_content_hash(clean_content))
        doc_identity_hash = str(
            metadata.get("source_hash")
            or metadata.get("document_content_hash")
            or metadata.get("doc_content_hash")
            or content_hash
        )
        doc_id = str(metadata.get("doc_id") or compute_doc_id(str(metadata["source_uri"]), doc_identity_hash))
        chunk_id = str(metadata.get("chunk_id", "0"))
        chunk_index = int(metadata.get("chunk_index", chunk_id))
        embedding_model = str(metadata.get("embedding_model", "unspecified"))
        chunker_version = str(metadata.get("chunker_version", "v1"))
        record_id = record.record_id or compute_record_id(doc_id, chunk_index)

        metadata["doc_id"] = doc_id
        metadata["content_hash"] = content_hash
        metadata.setdefault("source_hash", doc_identity_hash)
        metadata["chunk_id"] = chunk_id
        metadata["chunk_index"] = chunk_index
        metadata["embedding_model"] = embedding_model
        metadata["chunker_version"] = chunker_version
        metadata["record_id"] = record_id
        errors = validate_metadata(metadata)
        if errors:
            raise ValueError(f"invalid metadata: {'; '.join(errors)}")
        return Record(
            record_id=record_id, content=clean_content, metadata=metadata, lifecycle_state=metadata["lifecycle_state"]
        )

    async def upsert_records(self, collection: str, records: list[Record], provider_id: str | None = None) -> list[str]:
        """Handle upsert records."""
        pid = self._provider(provider_id)
        store = self._record_store(pid, collection)
        materialised = [self._normalise_record(collection, pid, record) for record in records]
        incoming_doc_ids = {str(record.metadata.get("doc_id", "")) for record in materialised}
        incoming_record_ids = {record.record_id for record in materialised}
        replacement_ids: dict[tuple[str, int], str] = {}
        doc_replacements: dict[str, str] = {}
        for record in materialised:
            doc_id = str(record.metadata.get("doc_id", ""))
            chunk_index = int(record.metadata.get("chunk_index", 0) or 0)
            if doc_id:
                replacement_ids[(doc_id, chunk_index)] = record.record_id
                doc_replacements.setdefault(doc_id, record.record_id)

        # Supersede prior versions from earlier writes only; keep records inside
        # the current batch active so multi-chunk ingestion remains searchable.
        for existing_id, existing in list(store.items()):
            doc_id = str(existing.metadata.get("doc_id", ""))
            if not doc_id or doc_id not in incoming_doc_ids or existing_id in incoming_record_ids:
                continue
            chunk_index = int(existing.metadata.get("chunk_index", 0) or 0)
            replacement_id = replacement_ids.get((doc_id, chunk_index), doc_replacements.get(doc_id))
            existing_meta = mark_superseded(existing.metadata, replacement_id)
            store[existing_id] = Record(
                record_id=existing.record_id,
                content=existing.content,
                metadata=existing_meta,
                lifecycle_state="superseded",
            )

        for normalised in materialised:
            store[normalised.record_id] = normalised
        return await self._adapter(provider_id).add_documents(
            collection,
            [r.content for r in materialised],
            [r.metadata for r in materialised],
            [r.record_id for r in materialised],
        )

    async def get_record(self, collection: str, record_id: str, provider_id: str | None = None) -> Record | None:
        """Return record."""
        pid = self._provider(provider_id)
        return self._record_store(pid, collection).get(record_id)

    async def list_records(
        self,
        collection: str,
        filters: dict | None = None,
        paging: dict | None = None,
        provider_id: str | None = None,
    ) -> list[Record]:
        """List records."""
        pid = self._provider(provider_id)
        filter_spec = coerce_metadata_filter(filters)
        paging = paging or {}
        offset = int(paging.get("offset", 0) or 0)
        limit = int(paging.get("limit", 100) or 100)

        out: list[Record] = []
        for record in self._record_store(pid, collection).values():
            if not matches_metadata(record.metadata, filter_spec):
                continue
            out.append(record)
        return out[offset : offset + limit]

    async def search(self, collection: str, request: SearchRequest, provider_id: str | None = None) -> SearchResponse:
        """Handle search."""
        pid = self._provider(provider_id)
        expected_dim = request.query_plan_hints.get("embedding_dim")
        expected_model = request.query_plan_hints.get("embedding_model")
        self._schema_versions.warn_if_query_mismatch(
            collection,
            expected_dimension_count=int(expected_dim) if expected_dim is not None else None,
            expected_embedding_model=str(expected_model) if expected_model else None,
        )
        merged_filter = dict(request.filters)
        merged_filter.setdefault("lifecycle_state", "active")
        filter_spec = coerce_metadata_filter(merged_filter)
        backend_filter = filter_to_backend_query(filter_spec)
        hits = await self._adapter(provider_id).search(
            collection,
            request.query_text or "*",
            request.top_k,
            backend_filter,
            {},
        )
        results: list[SearchResult] = []
        for hit in hits:
            score = float(hit.get("score", 0.0))
            if request.score_threshold is not None and score < request.score_threshold:
                continue
            record = self._record_store(pid, collection).get(hit["id"])
            if record and not matches_metadata(record.metadata, filter_spec):
                continue
            payload = dict(hit)
            if record:
                payload["metadata"] = dict(record.metadata)
            if not request.include_metadata:
                payload.pop("metadata", None)
            results.append(SearchResult(id=hit["id"], score=score, payload=payload))
        return SearchResponse(results=results[: request.top_k])

    async def search_stream(
        self, collection: str, request: SearchRequest, provider_id: str | None = None
    ) -> AsyncIterator[SearchResult]:
        """Handle search stream."""
        response = await self.search(collection, request, provider_id)
        for item in response.results:
            yield item

    async def delete_record(self, collection: str, record_id: str, provider_id: str | None = None) -> bool:
        """Delete record."""
        pid = self._provider(provider_id)
        current = self._record_store(pid, collection).get(record_id)
        if current is None:
            return False
        metadata = mark_deleted(current.metadata)
        self._record_store(pid, collection)[record_id] = Record(
            record_id=current.record_id,
            content=current.content,
            metadata=metadata,
            lifecycle_state="deleted",
        )
        return await self._adapter(provider_id).update_document(collection, record_id, current.content, metadata)

    async def update_record(
        self,
        collection: str,
        record_id: str,
        content: str,
        metadata: dict | None = None,
        provider_id: str | None = None,
    ) -> bool:
        """Update record."""
        pid = self._provider(provider_id)
        current = self._record_store(pid, collection).get(record_id)
        if current is None:
            return False
        merged = dict(current.metadata)
        if metadata:
            merged.update(
                {
                    key: value
                    for key, value in metadata.items()
                    if key not in {"parser_provider", "parser_version", "ocr_engine", "ocr_provider", "ocr_confidence",
                                   "page", "page_number", "section", "section_path", "table_id", "chunk_kind"}
                }
            )
            merged = merge_provenance(merged, metadata)
        errors = validate_metadata(merged)
        if errors:
            raise ValueError(f"invalid metadata: {'; '.join(errors)}")
        self._record_store(pid, collection)[record_id] = Record(
            record_id=record_id,
            content=content,
            metadata=merged,
            lifecycle_state=str(merged.get("lifecycle_state", "active")),
        )
        return await self._adapter(provider_id).update_document(collection, record_id, content, merged)

    async def delete_by_filter(self, collection: str, filters: dict, provider_id: str | None = None) -> int:
        """Delete by filter."""
        pid = self._provider(provider_id)
        to_delete = [
            r.record_id
            for r in await self.list_records(
                collection,
                filters,
                paging={"offset": 0, "limit": 1_000_000},
                provider_id=pid,
            )
        ]
        count = 0
        for record_id in to_delete:
            if await self.delete_record(collection, record_id, provider_id=pid):
                count += 1
        return count

    async def count_documents(
        self,
        collection: str,
        provider_id: str | None = None,
        filter: dict | None = None,
    ) -> int:
        """Handle count documents."""
        pid = self._provider(provider_id)
        if filter:
            return len(await self.list_records(collection, filter, provider_id=pid))
        return len(
            [r for r in self._record_store(pid, collection).values() if r.metadata.get("lifecycle_state") != "deleted"]
        )
