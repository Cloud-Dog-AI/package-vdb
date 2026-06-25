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

from cloud_dog_vdb.adapters.qdrant import QdrantAdapter
from cloud_dog_vdb.config.models import ProviderConfig
from cloud_dog_vdb.domain.models import CollectionSpec


@pytest.mark.asyncio
async def test_qdrant_crud(qdrant_ready):
    cfg = qdrant_ready
    a = QdrantAdapter(ProviderConfig(provider_id="qdrant", base_url=cfg.get("url", ""), api_key=cfg.get("api_key", "")))
    n = f"cloud_dog_ai_qdrant_{uuid.uuid4().hex[:10]}"
    try:
        await a.delete_collection(n)
        await a.create_collection(CollectionSpec(name=n, embedding_dim=4))
        await a.add_documents(n, ["hello"], [{}], ["d1"])
        assert await a.count_documents(n) == 1
        b = QdrantAdapter(
            ProviderConfig(provider_id="qdrant", base_url=cfg.get("url", ""), api_key=cfg.get("api_key", ""))
        )
        try:
            assert await b.count_documents(n) == 1
        finally:
            await b._client.aclose()
        await a.delete_document(n, "d1")
    finally:
        await a.delete_collection(n)
        await a._client.aclose()
