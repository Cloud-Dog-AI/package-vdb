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

from __future__ import annotations

import pytest

from cloud_dog_vdb.adapters.base import VDBAdapter
from cloud_dog_vdb.adapters.registry import AdapterRegistry
from cloud_dog_vdb.domain.models import CapabilityDescriptor, CollectionSpec
from cloud_dog_vdb.runtime.client import VDBClient
from tests.integration._metadata_parity import assert_metadata_field_parity


class _NoOpAdapter(VDBAdapter):
    def __init__(self, provider_id: str) -> None:
        self._provider_id = provider_id

    async def initialize(self, config: dict | None = None) -> bool:
        _ = config
        return True

    async def health_check(self) -> bool:
        return True

    async def create_collection(self, spec: CollectionSpec) -> dict:
        return {"name": spec.name}

    async def get_collection(self, name: str) -> dict | None:
        return {"name": name}

    async def delete_collection(self, name: str) -> bool:
        _ = name
        return True

    async def add_documents(
        self,
        collection: str,
        documents: list[str],
        metadatas: list[dict] | None = None,
        ids: list[str] | None = None,
    ) -> list[str]:
        _ = (collection, documents, metadatas)
        return list(ids or [])

    async def search(
        self,
        collection: str,
        query: str,
        n_results: int,
        filter: dict | None = None,
        search_options: dict | None = None,
    ) -> list[dict]:
        _ = (collection, query, n_results, filter, search_options)
        return []

    async def delete_document(self, collection: str, doc_id: str) -> bool:
        _ = (collection, doc_id)
        return True

    async def update_document(self, collection: str, doc_id: str, content: str, metadata: dict | None = None) -> bool:
        _ = (collection, doc_id, content, metadata)
        return True

    async def count_documents(self, collection: str, filter: dict | None = None) -> int:
        _ = (collection, filter)
        return 0

    def capabilities(self) -> CapabilityDescriptor:
        return CapabilityDescriptor(provider_id=self._provider_id)


@pytest.mark.asyncio
async def test_cross_backend_metadata_parity_fixture() -> None:
    registry = AdapterRegistry()
    registry.register("alpha", _NoOpAdapter("alpha"))
    registry.register("beta", _NoOpAdapter("beta"))
    client = VDBClient(registry, "alpha")

    fields = await assert_metadata_field_parity(
        client,
        provider_ids=["alpha", "beta"],
        metadata={
            "tenant_id": "tenant-a",
            "source_uri": "file://parity.txt",
            "source_type": "file",
            "access_tags": ["finance", "legal"],
        },
        filters={"tenant_id": "tenant-a", "namespace": "parity", "access_tags": ["finance"]},
    )

    assert {"tenant_id", "namespace", "record_id", "doc_id", "created_at", "access_tags"}.issubset(fields)
