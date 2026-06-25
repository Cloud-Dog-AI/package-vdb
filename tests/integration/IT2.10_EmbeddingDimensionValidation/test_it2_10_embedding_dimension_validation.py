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
async def test_it2_10_embedding_dim_hint_contract_real(chroma_ready: dict) -> None:
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
    collection = f"cloud_dog_ai_it2_dim_{uuid.uuid4().hex[:8]}"
    try:
        await client.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        await ingest_document(
            client,
            collection,
            b"embedding hint validation payload",
            source_uri="file://embedding.txt",
            options=ParserIngestionOptions(parser_chain=["internal"]),
            metadata={"tenant_id": "tenant_dim", "source_type": "file", "embedding_model": "test-model"},
        )
        with pytest.warns(RuntimeWarning, match="schema version mismatch"):
            response = await client.search(
                collection,
                SearchRequest(
                    query_text="payload",
                    top_k=3,
                    filters={"tenant_id": "tenant_dim"},
                    query_plan_hints={"embedding_dim": 4, "embedding_model": "test-model"},
                ),
            )
        assert len(response.results) >= 1
    finally:
        await client.delete_collection(collection)
