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
from cloud_dog_vdb import CollectionSpec, Record, SearchRequest, get_vdb_client


def _meta() -> dict:
    return {
        "tenant_id": "t1",
        "source_uri": "https://example.net/portable",
        "source_type": "web",
        "lifecycle_state": "active",
        "created_at": "2026-01-01T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_cross_backend_portable_contract_local():
    c = get_vdb_client(
        {
            "vector_stores": {
                "default_backend": "chroma",
                "chroma": {"enabled": True, "local_mode": True},
                "qdrant": {"enabled": False},
            }
        }
    )
    await c.create_collection(CollectionSpec(name="portable"))
    await c.upsert_records("portable", [Record("r1", "alpha", _meta())])
    r = await c.search("portable", SearchRequest(query_text="alpha", top_k=1))
    assert r.results and r.results[0].id == "r1"
