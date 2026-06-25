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
async def test_service_startup_pattern():
    c = get_vdb_client(
        {"vector_stores": {"default_backend": "chroma", "chroma": {"enabled": True, "local_mode": True}}}
    )
    assert await c.init_backend("dev")
    await c.create_collection(CollectionSpec(name="cloud_dog_ai_startup"))
    assert await c.get_collection("cloud_dog_ai_startup") is not None
