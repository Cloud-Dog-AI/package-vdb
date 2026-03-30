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

import pytest
from cloud_dog_vdb.adapters.chroma import ChromaAdapter
from cloud_dog_vdb.config.models import ProviderConfig
from cloud_dog_vdb.domain.models import CollectionSpec


@pytest.mark.asyncio
async def test_chroma_adapter_crud_search_count():
    a = ChromaAdapter(ProviderConfig(provider_id="chroma"), local_mode=True)
    await a.create_collection(CollectionSpec(name="c"))
    ids = await a.add_documents("c", ["hello world"], [{"tenant_id": "t1"}], ["d1"])
    assert ids == ["d1"]
    assert await a.count_documents("c") == 1
    assert (await a.search("c", "hello", 10))[0]["id"] == "d1"
    assert await a.update_document("c", "d1", "hello v2", {"tenant_id": "t1"})
    assert await a.delete_document("c", "d1")
