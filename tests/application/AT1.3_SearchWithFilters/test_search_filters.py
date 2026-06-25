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


def _meta(tenant: str) -> dict:
    return {
        "tenant_id": tenant,
        "source_uri": f"https://example.net/{tenant}",
        "source_type": "web",
        "lifecycle_state": "active",
        "created_at": "2026-01-01T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_search_with_filters(chroma_ready):
    cfg = chroma_ready
    collection = f"cloud_dog_ai_filters_{uuid.uuid4().hex[:10]}"
    c = get_vdb_client(
        {
            "vector_stores": {
                "default_backend": "chroma",
                "chroma": {
                    "enabled": True,
                    "local_mode": False,
                    "base_url": cfg.get("base_url", ""),
                    "auth_token": cfg.get("auth_token", ""),
                },
            }
        }
    )
    try:
        await c.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        await c.upsert_records(
            collection,
            [
                Record("a", "hello", _meta("t1")),
                Record("b", "hello", _meta("t2")),
            ],
        )
        rsp = await c.search(collection, SearchRequest(query_text="hello", top_k=5, filters={"tenant_id": "t1"}))
        assert len(rsp.results) == 1 and rsp.results[0].id == "a"
    finally:
        await c.delete_collection(collection)
