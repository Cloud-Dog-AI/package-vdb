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

from cloud_dog_vdb.adapters.chroma import ChromaAdapter
from cloud_dog_vdb.config.models import ProviderConfig
from cloud_dog_vdb.domain.models import CollectionSpec


def _meta(state: str) -> dict:
    return {
        "tenant_id": "t1",
        "source_uri": "https://example.net/lifecycle",
        "source_type": "web",
        "lifecycle_state": state,
        "created_at": "2026-01-01T00:00:00Z",
        "is_latest": state == "active",
    }


@pytest.mark.asyncio
async def test_lifecycle_real_backend_pattern(chroma_ready):
    cfg = chroma_ready
    a = ChromaAdapter(
        ProviderConfig(provider_id="chroma", base_url=cfg.get("base_url", ""), api_key=cfg.get("auth_token", "")),
        local_mode=False,
    )
    collection = f"cloud_dog_ai_lifecycle_{uuid.uuid4().hex[:10]}"
    try:
        await a.delete_collection(collection)
        await a.create_collection(CollectionSpec(name=collection, embedding_dim=4))

        await a.add_documents(collection, ["alpha"], [_meta("active")], ["d1"])
        active_before = await a.search(collection, "alpha", 5, {"lifecycle_state": "active"})
        assert any(item["id"] == "d1" for item in active_before)

        assert await a.update_document(collection, "d1", "alpha", _meta("deleted"))
        active_after_delete = await a.search(collection, "alpha", 5, {"lifecycle_state": "active"})
        assert all(item["id"] != "d1" for item in active_after_delete)

        await a.add_documents(collection, ["beta"], [_meta("active")], ["d2"])
        assert await a.update_document(collection, "d2", "beta", _meta("superseded"))
        superseded = await a.search(collection, "beta", 5, {"lifecycle_state": "superseded"})
        assert any(item["id"] == "d2" for item in superseded)
    finally:
        await a.delete_collection(collection)
        await a._client.aclose()
