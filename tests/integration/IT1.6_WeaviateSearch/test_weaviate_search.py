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

from cloud_dog_vdb.adapters.weaviate import WeaviateAdapter
from cloud_dog_vdb.config.models import ProviderConfig
from cloud_dog_vdb.domain.models import CollectionSpec


@pytest.mark.asyncio
async def test_weaviate_search(weaviate_ready):
    cfg = weaviate_ready
    a = WeaviateAdapter(
        ProviderConfig(provider_id="weaviate", base_url=cfg.get("url", ""), api_key=cfg.get("api_key", ""))
    )
    n = f"cloud_dog_ai_weaviate_search_{uuid.uuid4().hex[:10]}"
    try:
        await a.delete_collection(n)
        await a.create_collection(CollectionSpec(name=n, embedding_dim=4))
        await a.add_documents(n, ["alpha"], [{}], ["a"])
        assert (await a.search(n, "alpha", 1))[0]["id"] == "a"
    finally:
        await a.delete_collection(n)
        await a._client.aclose()
