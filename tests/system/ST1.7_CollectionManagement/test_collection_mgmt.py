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
from cloud_dog_vdb import CollectionSpec, get_vdb_client


@pytest.mark.asyncio
async def test_collection_management_cycle():
    c = get_vdb_client(
        {"vector_stores": {"default_backend": "chroma", "chroma": {"enabled": True, "local_mode": True}}}
    )
    await c.create_collection(CollectionSpec(name="m"))
    assert await c.get_collection("m") is not None
    collections = await c.list_collections()
    assert len(collections) == 1
    updated = await c.update_collection("m", {"namespace": "tenant-a", "metadata": {"owner": "ops"}})
    assert updated is not None
    assert updated["namespace"] == "tenant-a"
    assert updated["metadata"]["owner"] == "ops"
    assert await c.delete_collection("m")
