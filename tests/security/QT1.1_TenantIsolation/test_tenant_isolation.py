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


def _meta(tenant_id: str) -> dict:
    return {
        "tenant_id": tenant_id,
        "source_uri": f"https://example.net/{tenant_id}",
        "source_type": "web",
        "lifecycle_state": "active",
        "created_at": "2026-01-01T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_tenant_isolation_security(chroma_ready):
    cfg = chroma_ready
    a = ChromaAdapter(
        ProviderConfig(provider_id="chroma", base_url=cfg.get("base_url", ""), api_key=cfg.get("auth_token", "")),
        local_mode=False,
    )
    collection = f"cloud_dog_ai_tenant_isolation_{uuid.uuid4().hex[:10]}"
    try:
        await a.delete_collection(collection)
        await a.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        await a.add_documents(
            collection,
            ["hello t1", "hello t2"],
            [_meta("t1"), _meta("t2")],
            ["t1-record", "t2-record"],
        )
        tenant_t1 = await a.search(collection, "hello", 10, {"tenant_id": "t1"})
        assert {item["id"] for item in tenant_t1} == {"t1-record"}
    finally:
        await a.delete_collection(collection)
        await a._client.aclose()
