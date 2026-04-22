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

from cloud_dog_vdb.domain.models import CapabilityDescriptor
from cloud_dog_vdb.domain.models import CollectionSpec
from cloud_dog_vdb.adapters.qdrant import QdrantAdapter
from cloud_dog_vdb.config.models import ProviderConfig


def test_qdrant_capability_shape():
    c = QdrantAdapter(ProviderConfig(provider_id="qdrant", base_url="https://qdrant.test"))
    assert isinstance(c.capabilities(), CapabilityDescriptor)


class _FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeClient:
    def __init__(self) -> None:
        self.get_statuses = [404, 200]
        self.point_statuses = [200]
        self.put_calls: list[tuple[str, dict]] = []
        self.get_calls: list[str] = []

    async def put(self, url: str, *, headers: dict[str, str], json: dict) -> _FakeResponse:
        self.put_calls.append((url, json))
        if url.endswith("/points?wait=true"):
            status = self.point_statuses.pop(0) if self.point_statuses else 200
            return _FakeResponse(status)
        return _FakeResponse(200)

    async def get(self, url: str, *, headers: dict[str, str]) -> _FakeResponse:
        self.get_calls.append(url)
        status = self.get_statuses.pop(0) if self.get_statuses else 200
        return _FakeResponse(status)


@pytest.mark.asyncio
async def test_qdrant_create_collection_waits_until_collection_is_queryable() -> None:
    adapter = QdrantAdapter(ProviderConfig(provider_id="qdrant", base_url="https://qdrant.test", timeout_seconds=1.0))
    fake_client = _FakeClient()
    adapter._client = fake_client  # noqa: SLF001 - unit test validates adapter transport sequencing

    result = await adapter.create_collection(CollectionSpec(name="ready-check", embedding_dim=8))

    assert result == {"name": "ready-check", "status": "created"}
    assert fake_client.put_calls
    assert fake_client.get_calls == [
        "https://qdrant.test/collections/ready-check",
        "https://qdrant.test/collections/ready-check",
    ]


@pytest.mark.asyncio
async def test_qdrant_add_documents_retries_once_after_initial_404() -> None:
    adapter = QdrantAdapter(ProviderConfig(provider_id="qdrant", base_url="https://qdrant.test", timeout_seconds=1.0))
    fake_client = _FakeClient()
    fake_client.point_statuses = [404, 200]
    adapter._client = fake_client  # noqa: SLF001 - unit test validates adapter retry behaviour
    adapter._dims["ready-check"] = 4  # noqa: SLF001 - test seeds collection dimensions directly

    ids = await adapter.add_documents("ready-check", ["alpha"], [{"source": "unit"}], ["doc-1"])

    assert ids == ["doc-1"]
    point_calls = [url for url, _json in fake_client.put_calls if url.endswith("/points?wait=true")]
    assert point_calls == [
        "https://qdrant.test/collections/ready-check/points?wait=true",
        "https://qdrant.test/collections/ready-check/points?wait=true",
    ]
