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
from tests.integration._parser_ingest_helpers import _corpus_path


@pytest.mark.asyncio
async def test_it2_7_delete_by_filter_fallback_real(
    chroma_ready: dict,
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, str]],
) -> None:
    mineru = services["mineru"]
    if not mineru.get("enabled"):
        pytest.fail("MINERU_ENABLED must be true for IT2.7", pytrace=False)

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
    collection = f"cloud_dog_ai_it2_delete_{uuid.uuid4().hex[:8]}"
    source_path = _corpus_path(corpus_entries)
    source_name = source_path.name
    try:
        await client.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        await ingest_document(
            client,
            collection,
            source_path.read_bytes(),
            record_prefix="tenant-a",
            source_uri=f"file://{source_name}",
            options=ParserIngestionOptions(parser_chain=["mineru", "internal"]),
            parser_services={
                "mineru": {
                    "base_url": str(mineru.get("base_url", "")),
                    "api_key": str(mineru.get("api_key", "")),
                    "timeout_seconds": float(mineru.get("timeout_seconds", 120.0) or 120.0),
                }
            },
            metadata={"tenant_id": "tenant_a", "source_type": "file"},
        )
        await ingest_document(
            client,
            collection,
            b"plain text second tenant",
            record_prefix="tenant-b",
            source_uri="file://tenant-b.txt",
            options=ParserIngestionOptions(parser_chain=["internal"]),
            metadata={"tenant_id": "tenant_b", "source_type": "file"},
        )

        deleted = await client.delete_by_filter(collection, {"tenant_id": "tenant_a"})
        assert deleted >= 1
        response = await client.search(
            collection,
            SearchRequest(query_text="*", top_k=20, filters={"tenant_id": "tenant_a"}),
        )
        assert len(response.results) == 0
    finally:
        await client.delete_collection(collection)
