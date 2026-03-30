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

from abc import ABC, abstractmethod
from typing import Any

from cloud_dog_vdb.domain.models import CapabilityDescriptor, CollectionSpec


class VDBAdapter(ABC):
    """Define the asynchronous vector-database adapter contract."""

    @abstractmethod
    async def initialize(self, config: dict[str, Any] | None = None) -> bool:
        """Initialise the adapter with optional configuration."""
        raise NotImplementedError

    @abstractmethod
    async def health_check(self) -> bool:
        """Return whether the adapter is healthy."""
        raise NotImplementedError

    @abstractmethod
    async def create_collection(self, spec: CollectionSpec) -> dict:
        """Create collection."""
        raise NotImplementedError

    @abstractmethod
    async def get_collection(self, name: str) -> dict | None:
        """Return collection."""
        raise NotImplementedError

    @abstractmethod
    async def delete_collection(self, name: str) -> bool:
        """Delete collection."""
        raise NotImplementedError

    @abstractmethod
    async def add_documents(
        self,
        collection: str,
        documents: list[str],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
    ) -> list[str]:
        """Add documents to a collection and return their identifiers."""
        raise NotImplementedError

    @abstractmethod
    async def search(
        self,
        collection: str,
        query: str,
        n_results: int,
        filter: dict[str, Any] | None = None,
        search_options: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Search a collection and return matching records."""
        raise NotImplementedError

    @abstractmethod
    async def delete_document(self, collection: str, doc_id: str) -> bool:
        """Delete document."""
        raise NotImplementedError

    @abstractmethod
    async def update_document(
        self, collection: str, doc_id: str, content: str, metadata: dict[str, Any] | None = None
    ) -> bool:
        """Update document."""
        raise NotImplementedError

    @abstractmethod
    async def count_documents(self, collection: str, filter: dict[str, Any] | None = None) -> int:
        """Count documents in a collection."""
        raise NotImplementedError

    @abstractmethod
    def capabilities(self) -> CapabilityDescriptor:
        """Return the adapter capability descriptor."""
        raise NotImplementedError
