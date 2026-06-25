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
async def test_it2_9_metadata_filter_behaviour_real(chroma_ready: dict) -> None:
    client = get_vdb_client(
        {
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
    )
    collection = f"cloud_dog_ai_it2_filters_{uuid.uuid4().hex[:8]}"
    try:
        await client.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        await ingest_document(
            client,
            collection,
            b"metadata tenant alpha",
            record_prefix="alpha",
            source_uri="file://a.txt",
            options=ParserIngestionOptions(parser_chain=["internal"]),
            metadata={"tenant_id": "tenant_alpha", "source_type": "file"},
        )
        await ingest_document(
            client,
            collection,
            b"metadata tenant beta",
            record_prefix="beta",
            source_uri="file://b.txt",
            options=ParserIngestionOptions(parser_chain=["internal"]),
            metadata={"tenant_id": "tenant_beta", "source_type": "file"},
        )
        alpha = await client.search(
            collection,
            SearchRequest(query_text="*", top_k=10, filters={"tenant_id": "tenant_alpha"}),
        )
        beta = await client.search(
            collection,
            SearchRequest(query_text="*", top_k=10, filters={"tenant_id": "tenant_beta"}),
        )
        assert len(alpha.results) >= 1
        assert len(beta.results) >= 1
        assert alpha.results[0].payload.get("metadata", {}).get("tenant_id") == "tenant_alpha"
        assert beta.results[0].payload.get("metadata", {}).get("tenant_id") == "tenant_beta"
    finally:
        await client.delete_collection(collection)
