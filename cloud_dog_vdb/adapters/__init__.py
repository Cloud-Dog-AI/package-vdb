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

from cloud_dog_vdb.adapters.base import VDBAdapter
from cloud_dog_vdb.adapters.chroma import ChromaAdapter
from cloud_dog_vdb.adapters.factory import build_adapter
from cloud_dog_vdb.adapters.infinity import InfinityAdapter
from cloud_dog_vdb.adapters.opensearch import OpenSearchAdapter
from cloud_dog_vdb.adapters.qdrant import QdrantAdapter
from cloud_dog_vdb.adapters.registry import AdapterRegistry
from cloud_dog_vdb.adapters.weaviate import WeaviateAdapter

try:  # optional dependency (asyncpg)
    from cloud_dog_vdb.adapters.pgvector import PGVectorAdapter
except Exception:  # pragma: no cover
    PGVectorAdapter = None  # type: ignore[assignment]

__all__ = [
    "VDBAdapter",
    "AdapterRegistry",
    "build_adapter",
    "ChromaAdapter",
    "QdrantAdapter",
    "WeaviateAdapter",
    "OpenSearchAdapter",
    "InfinityAdapter",
    "PGVectorAdapter",
]
