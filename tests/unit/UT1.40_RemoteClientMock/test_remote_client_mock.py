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

from cloud_dog_vdb.domain.models import SearchRequest
from cloud_dog_vdb.remote.client import VDBClient


@pytest.mark.asyncio
async def test_client_only_remote_integration_search():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/health"):
            return httpx.Response(200, json={"ok": True})
        if request.url.path.endswith("/search"):
            return httpx.Response(
                200,
                headers={"X-VDB-Backend": "portable"},
                json={"results": [{"id": "r1", "score": 0.99, "content": "doc", "metadata": {"tenant_id": "t1"}}]},
            )
        return httpx.Response(404, json={})

    client = VDBClient("https://remote-vdb.local", backend_hint="portable")
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    assert await client.health_check() is True
    result = await client.search("cloud_dog_ai_docs", SearchRequest(query_text="hello", top_k=5))
    assert result.results[0].id == "r1"
    assert result.results[0].payload["metadata"]["tenant_id"] == "t1"
    await client.close()


@pytest.mark.asyncio
async def test_client_only_remote_integration_connection_failure():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "unavailable"})

    client = VDBClient("https://remote-vdb.local")
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(RuntimeError):
        await client.search("cloud_dog_ai_docs", SearchRequest(query_text="hello"))
    await client.close()
