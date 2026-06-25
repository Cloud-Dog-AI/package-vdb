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

import pytest

from cloud_dog_vdb import CollectionSpec, SearchRequest, get_vdb_client
from cloud_dog_vdb.ingestion.pipeline import ParserIngestionOptions, ingest_document


@pytest.mark.asyncio
async def test_it2_8_lifecycle_mark_deleted_real(
    chroma_ready: dict,
    services: dict[str, dict[str, object]],
) -> None:
    mineru = services["mineru"]
    if not mineru.get("enabled"):
        pytest.fail("MINERU_ENABLED must be true for IT2.8", pytrace=False)

    config = {
        "vector_stores": {
            "default_backend": "chroma",
            "chroma": {
                "enabled": True,
                "local_mode": False,
                "base_url": chroma_ready.get("base_url", ""),
                "auth_token": chroma_ready.get("auth_token", ""),
            },
        }
    }
    client = get_vdb_client(config)
    collection = f"cloud_dog_ai_it2_lifecycle_{uuid.uuid4().hex[:8]}"
    try:
        await client.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        ids = await ingest_document(
            client,
            collection,
            b"lifecycle integration payload",
            source_uri="file://lifecycle.txt",
            options=ParserIngestionOptions(parser_chain=["internal"]),
            metadata={"tenant_id": "tenant_lifecycle", "source_type": "file"},
        )
        assert ids
        deleted = await client.delete_record(collection, ids[0])
        assert deleted is True

        records = await client.list_records(collection, {"record_id": ids[0]})
        assert records
        assert str(records[0].metadata.get("lifecycle_state", "")) == "deleted"

        active = await client.search(
            collection,
            SearchRequest(query_text="*", top_k=5, filters={"tenant_id": "tenant_lifecycle"}),
        )
        assert len(active.results) == 0
    finally:
        await client.delete_collection(collection)
