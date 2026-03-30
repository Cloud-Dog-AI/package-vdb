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

import pytest

from cloud_dog_vdb import CollectionSpec, Record, get_vdb_client


def _meta(*, chunk_id: str) -> dict[str, object]:
    return {
        "tenant_id": "tenant-upsert",
        "source_uri": "file://batch-upsert.pdf",
        "source_type": "file",
        "lifecycle_state": "active",
        "created_at": "2026-01-01T00:00:00Z",
        "doc_id": "doc-batch-upsert",
        "chunk_id": chunk_id,
    }


@pytest.mark.asyncio
async def test_ut2_13_batch_upsert_keeps_multi_chunk_records_active() -> None:
    client = get_vdb_client(
        {"vector_stores": {"default_backend": "chroma", "chroma": {"enabled": True, "local_mode": True}}}
    )
    collection = "ut2_batch_upsert_lifecycle_parity"

    await client.create_collection(CollectionSpec(name=collection, embedding_dim=4))
    ids = await client.upsert_records(
        collection,
        [
            Record(record_id="chunk-0", content="first chunk", metadata=_meta(chunk_id="0")),
            Record(record_id="chunk-1", content="second chunk", metadata=_meta(chunk_id="1")),
        ],
    )

    assert ids == ["chunk-0", "chunk-1"]
    stored = await client.list_records(collection, {"tenant_id": "tenant-upsert"}, {"offset": 0, "limit": 10})
    assert {record.record_id for record in stored} == {"chunk-0", "chunk-1"}
    assert all(str(record.metadata.get("lifecycle_state", "")) == "active" for record in stored)
    assert all(record.metadata.get("is_latest") is True for record in stored)


@pytest.mark.asyncio
async def test_ut2_13_upsert_sanitizes_nul_bytes_in_content() -> None:
    client = get_vdb_client(
        {"vector_stores": {"default_backend": "chroma", "chroma": {"enabled": True, "local_mode": True}}}
    )
    collection = "ut2_nul_sanitization"

    await client.create_collection(CollectionSpec(name=collection, embedding_dim=4))
    await client.upsert_records(
        collection,
        [
            Record(
                record_id="nul-1",
                content="alpha\x00beta",
                metadata={
                    "tenant_id": "tenant-upsert",
                    "source_uri": "file://nul.txt",
                    "source_type": "file",
                    "lifecycle_state": "active",
                    "created_at": "2026-01-01T00:00:00Z",
                },
            )
        ],
    )

    stored = await client.list_records(collection, {"tenant_id": "tenant-upsert"}, {"offset": 0, "limit": 10})
    assert len(stored) == 1
    assert "\x00" not in stored[0].content
    assert stored[0].content == "alphabeta"
