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

from cloud_dog_vdb.domain.models import CollectionSpec


class CollectionManager:
    """Represent collection manager."""

    def __init__(self, vdb_client) -> None:
        self.vdb = vdb_client

    async def create(self, spec: CollectionSpec, provider_id: str | None = None) -> dict:
        """Handle create."""
        return await self.vdb.create_collection(spec, provider_id)

    async def get(self, name: str, provider_id: str | None = None) -> dict | None:
        """Handle get."""
        return await self.vdb.get_collection(name, provider_id)

    async def list(self, provider_id: str | None = None) -> list[dict]:
        """Handle list."""
        return await self.vdb.list_collections(provider_id)

    async def update(self, name: str, patch: dict, provider_id: str | None = None) -> dict | None:
        """Handle update."""
        return await self.vdb.update_collection(name, patch, provider_id)

    async def delete(self, name: str, provider_id: str | None = None) -> bool:
        """Handle delete."""
        return await self.vdb.delete_collection(name, provider_id)
