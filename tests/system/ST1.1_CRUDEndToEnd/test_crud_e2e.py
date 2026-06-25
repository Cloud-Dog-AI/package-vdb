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
from cloud_dog_vdb import CollectionSpec, Record, get_vdb_client


def _meta(tenant: str = "t1") -> dict:
    return {
        "tenant_id": tenant,
        "source_uri": f"https://example.net/{tenant}",
        "source_type": "web",
        "lifecycle_state": "active",
        "created_at": "2026-01-01T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_crud_end_to_end_local():
    c = get_vdb_client(
        {"vector_stores": {"default_backend": "chroma", "chroma": {"enabled": True, "local_mode": True}}}
    )
    await c.create_collection(CollectionSpec(name="c"))
    await c.upsert_records("c", [Record("r1", "hello", _meta())])
    assert await c.count_documents("c") == 1
    records = await c.list_records("c", {"tenant_id": "t1"})
    assert len(records) == 1
    assert await c.update_record("c", "r1", "hello2", _meta())
    record = await c.get_record("c", "r1")
    assert record is not None
    assert record.content == "hello2"
    assert await c.delete_by_filter("c", {"tenant_id": "t1"}) == 1
    assert await c.count_documents("c") == 0
