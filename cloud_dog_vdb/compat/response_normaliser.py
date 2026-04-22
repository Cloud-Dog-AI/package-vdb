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

# cloud_dog_vdb — Response normaliser
"""Normalise backend-specific response payloads to unified domain models."""

from __future__ import annotations

from typing import Any

from cloud_dog_vdb.domain.models import Record, SearchResponse, SearchResult


class ResponseNormaliser:
    """Normalise legacy backend response schemas."""

    def normalise_search(self, backend: str, payload: Any) -> SearchResponse:
        """Handle normalise search."""
        name = backend.strip().lower()
        if name == "chroma":
            return SearchResponse(results=self._from_chroma(payload))
        if name == "qdrant":
            return SearchResponse(results=self._from_qdrant(payload))
        if name == "weaviate":
            return SearchResponse(results=self._from_weaviate(payload))
        if name == "opensearch":
            return SearchResponse(results=self._from_opensearch(payload))
        if name == "pgvector":
            return SearchResponse(results=self._from_pgvector(payload))
        if name == "portable":
            return SearchResponse(results=self._from_portable(payload))
        raise ValueError(f"Unsupported backend: {backend}")

    def normalise_record(self, backend: str, payload: dict[str, Any]) -> Record:
        """Handle normalise record."""
        if backend.strip().lower() == "portable":
            return Record(
                record_id=str(payload.get("record_id", payload.get("id", ""))),
                content=str(payload.get("content", "")),
                metadata=dict(payload.get("metadata", {})),
                lifecycle_state=str(payload.get("lifecycle_state", "active")),
            )
        response = self.normalise_search(backend, [payload])
        if not response.results:
            raise ValueError("Unable to normalise record payload")
        item = response.results[0]
        metadata = dict(item.payload.get("metadata", {}))
        return Record(
            record_id=item.id,
            content=str(item.payload.get("content", "")),
            metadata=metadata,
            lifecycle_state=str(metadata.get("lifecycle_state", "active")),
        )

    @staticmethod
    def _result(item_id: Any, score: Any, content: Any = "", metadata: Any = None) -> SearchResult:
        meta = metadata if isinstance(metadata, dict) else {}
        return SearchResult(
            id=str(item_id),
            score=float(score or 0.0),
            payload={"content": str(content or ""), "metadata": meta},
        )

    def _from_portable(self, payload: Any) -> list[SearchResult]:
        if isinstance(payload, dict):
            items = payload.get("results", [])
        else:
            items = payload or []
        out: list[SearchResult] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            out.append(
                self._result(
                    item.get("id"),
                    item.get("score"),
                    item.get("content", item.get("payload", {}).get("content", "")),
                    item.get("metadata", item.get("payload", {}).get("metadata", {})),
                )
            )
        return out

    def _from_chroma(self, payload: Any) -> list[SearchResult]:
        if isinstance(payload, list):
            return self._from_portable(payload)
        ids = ((payload or {}).get("ids") or [[]])[0]
        docs = ((payload or {}).get("documents") or [[]])[0]
        metas = ((payload or {}).get("metadatas") or [[]])[0]
        distances = ((payload or {}).get("distances") or [[]])[0]
        out: list[SearchResult] = []
        for idx, item_id in enumerate(ids):
            distance = float(distances[idx]) if idx < len(distances) else 1.0
            out.append(
                self._result(
                    item_id,
                    1.0 - distance,
                    docs[idx] if idx < len(docs) else "",
                    metas[idx] if idx < len(metas) else {},
                )
            )
        return out

    def _from_qdrant(self, payload: Any) -> list[SearchResult]:
        items = payload.get("result") if isinstance(payload, dict) else payload
        if isinstance(items, dict):
            items = items.get("points", [])
        items = items or []
        out: list[SearchResult] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            payload_block = item.get("payload", {})
            metadata = payload_block.get("metadata", {})
            out.append(
                self._result(
                    payload_block.get("external_id", item.get("id")),
                    item.get("score", 0.0),
                    payload_block.get("text", ""),
                    metadata,
                )
            )
        return out

    def _from_weaviate(self, payload: Any) -> list[SearchResult]:
        if isinstance(payload, list):
            items = payload
        else:
            items = ((payload or {}).get("data") or {}).get("Get") or (payload or {}).get("objects") or []
            if isinstance(items, dict):
                first_key = next(iter(items.keys()), None)
                items = items.get(first_key, []) if first_key else []
        out: list[SearchResult] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            extra = item.get("_additional", {})
            distance = float(extra.get("distance", 1.0))
            out.append(
                self._result(
                    item.get("external_id", extra.get("id")),
                    1.0 - distance,
                    item.get("text", ""),
                    item.get("metadata", {}),
                )
            )
        return out

    def _from_opensearch(self, payload: Any) -> list[SearchResult]:
        if isinstance(payload, list):
            return self._from_portable(payload)
        hits = ((payload or {}).get("hits") or {}).get("hits") or []
        out: list[SearchResult] = []
        for hit in hits:
            if not isinstance(hit, dict):
                continue
            source = hit.get("_source", {})
            out.append(
                self._result(
                    hit.get("_id"),
                    hit.get("_score", 0.0),
                    source.get("text", ""),
                    source.get("metadata", {}),
                )
            )
        return out

    def _from_pgvector(self, payload: Any) -> list[SearchResult]:
        if isinstance(payload, list):
            return self._from_portable(payload)
        rows = payload.get("rows", []) if isinstance(payload, dict) else []
        out: list[SearchResult] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            out.append(
                self._result(
                    row.get("id"),
                    row.get("score", 0.0),
                    row.get("content", ""),
                    row.get("metadata", {}),
                )
            )
        return out
