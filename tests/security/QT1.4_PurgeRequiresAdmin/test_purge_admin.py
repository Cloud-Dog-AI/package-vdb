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

from cloud_dog_vdb.access.enforcement import can_admin
from cloud_dog_vdb.access.policy import AccessPolicy
from cloud_dog_vdb.adapters.chroma import ChromaAdapter
from cloud_dog_vdb.config.models import ProviderConfig
from cloud_dog_vdb.domain.models import CollectionSpec


@pytest.mark.asyncio
async def test_purge_requires_admin_role(chroma_ready):
    cfg = chroma_ready
    policy = AccessPolicy(admins={"ops"})

    a = ChromaAdapter(
        ProviderConfig(provider_id="chroma", base_url=cfg.get("base_url", ""), api_key=cfg.get("auth_token", "")),
        local_mode=False,
    )
    collection = f"cloud_dog_ai_purge_admin_{uuid.uuid4().hex[:10]}"
    try:
        await a.delete_collection(collection)
        await a.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        await a.add_documents(collection, ["doc"], [{"tenant_id": "t1"}], ["doc-1"])

        assert can_admin("user", policy) is False
        non_admin_attempt = can_admin("user", policy)
        assert non_admin_attempt is False
        assert await a.count_documents(collection) == 1

        assert can_admin("ops", policy) is True
        assert await a.delete_collection(collection) is True
        assert await a.get_collection(collection) is None
    finally:
        await a.delete_collection(collection)
        await a._client.aclose()
