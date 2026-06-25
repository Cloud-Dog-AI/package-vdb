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

from cloud_dog_vdb import CollectionSpec, get_vdb_client


@pytest.mark.asyncio
async def test_service_startup_pattern(chroma_ready):
    cfg = chroma_ready
    collection = f"cloud_dog_ai_startup_{uuid.uuid4().hex[:10]}"
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
        assert await c.init_backend("dev")
        await c.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        assert await c.get_collection(collection) is not None
    finally:
        await c.delete_collection(collection)
