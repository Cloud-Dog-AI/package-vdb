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

# cloud_dog_vdb — Adapter factory
"""Factory helpers for constructing VDB adapters from provider config."""

from __future__ import annotations

from cloud_dog_vdb.adapters.base import VDBAdapter
from cloud_dog_vdb.config.models import ProviderConfig


def build_adapter(config: ProviderConfig, *, local_mode: bool = False) -> VDBAdapter:
    """Build adapter."""
    provider = config.provider_id.lower()
    if provider == "chroma":
        from cloud_dog_vdb.adapters.chroma import ChromaAdapter

        return ChromaAdapter(config, local_mode=local_mode)
    if provider == "qdrant":
        from cloud_dog_vdb.adapters.qdrant import QdrantAdapter

        return QdrantAdapter(config, local_mode=local_mode)
    if provider == "weaviate":
        from cloud_dog_vdb.adapters.weaviate import WeaviateAdapter

        return WeaviateAdapter(config, local_mode=local_mode)
    if provider == "opensearch":
        from cloud_dog_vdb.adapters.opensearch import OpenSearchAdapter

        return OpenSearchAdapter(config, local_mode=local_mode)
    if provider == "pgvector":
        from cloud_dog_vdb.adapters.pgvector import PGVectorAdapter

        return PGVectorAdapter(config, local_mode=local_mode)
    if provider == "infinity":
        from cloud_dog_vdb.adapters.infinity import InfinityAdapter

        return InfinityAdapter(config, local_mode=local_mode)
    raise ValueError(f"Unsupported provider: {config.provider_id}")
