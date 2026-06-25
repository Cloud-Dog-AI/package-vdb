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

import uuid

import pytest

from cloud_dog_vdb import CollectionSpec, Record, SearchRequest, get_vdb_client


def _meta(provider_id: str) -> dict:
    return {
        "tenant_id": "t1",
        "source_uri": f"https://example.net/portable/{provider_id}",
        "source_type": "web",
        "lifecycle_state": "active",
        "created_at": "2026-01-01T00:00:00Z",
    }


async def _run_portable_contract(provider_id: str, cfg: dict) -> None:
    collection = f"cloud_dog_ai_portable_{provider_id}_{uuid.uuid4().hex[:10]}"
    c = get_vdb_client(
        {
            "vector_stores": {
                "default_backend": provider_id,
                provider_id: {
                    "enabled": True,
                    "local_mode": False,
                    **cfg,
                },
            }
        }
    )
    try:
        await c.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        await c.upsert_records(collection, [Record("r1", "alpha", _meta(provider_id))])
        r = await c.search(collection, SearchRequest(query_text="alpha", top_k=1))
        assert r.results and r.results[0].id == "r1"
    finally:
        await c.delete_collection(collection)


@pytest.mark.asyncio
async def test_cross_backend_portable_contract_real(chroma_ready, qdrant_ready):
    await _run_portable_contract("chroma", chroma_ready)
    await _run_portable_contract("qdrant", qdrant_ready)
