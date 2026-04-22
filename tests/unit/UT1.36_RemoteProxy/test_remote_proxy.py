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

import httpx
import pytest

from cloud_dog_vdb.domain.models import Record, SearchRequest
from cloud_dog_vdb.remote.client import VDBClient


@pytest.mark.asyncio
async def test_remote_proxy_search_upsert_delete():
    state = {"deleted": False}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/search"):
            return httpx.Response(
                200,
                headers={"X-VDB-Backend": "opensearch"},
                json={
                    "hits": {
                        "hits": [
                            {"_id": "r1", "_score": 1.0, "_source": {"text": "hello", "metadata": {"tenant_id": "t1"}}}
                        ]
                    }
                },
            )
        if request.url.path.endswith("/records:upsert"):
            return httpx.Response(200, json={"ids": ["r1"]})
        if request.method == "DELETE":
            state["deleted"] = True
            return httpx.Response(200, json={"deleted": True})
        if request.url.path.endswith("/health"):
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(404, json={})

    client = VDBClient("https://remote-vdb.local")
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    search = await client.search("docs", SearchRequest(query_text="hello", top_k=3))
    assert search.results and search.results[0].id == "r1"
    ids = await client.upsert_records("docs", [Record(record_id="r1", content="hello", metadata={})])
    assert ids == ["r1"]
    assert await client.delete_record("docs", "r1")
    assert state["deleted"] is True
    await client.close()


@pytest.mark.asyncio
async def test_remote_proxy_handles_remote_error():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "bad"})

    client = VDBClient("https://remote-vdb.local")
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(RuntimeError):
        await client.search("docs", SearchRequest(query_text="x"))
    await client.close()
