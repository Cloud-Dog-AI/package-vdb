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

from dataclasses import dataclass, field
from typing import Any

from cloud_dog_vdb.domain.enums import DistanceMetric


@dataclass(frozen=True, slots=True)
class CollectionSpec:
    """Represent collection spec."""
    name: str
    namespace: str = ""
    embedding_dim: int = 1024
    distance_metric: DistanceMetric = DistanceMetric.COSINE
    metadata: dict[str, Any] = field(default_factory=dict)
    metadata_schema: dict[str, Any] = field(default_factory=dict)
    index_params: dict[str, Any] = field(default_factory=dict)
    access_policy: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Record:
    """Represent record."""
    record_id: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)
    lifecycle_state: str = "active"


@dataclass(frozen=True, slots=True)
class CapabilityDescriptor:
    """Represent capability descriptor."""
    provider_id: str
    filtering: bool = True
    hybrid_search: bool = False
    sparse_vectors: bool = False
    multi_vector: bool = False
    metadata_indexing: bool = True
    upsert_semantics: bool = True
    delete_by_filter: bool = False
    ttl_native: bool = False
    transactions: bool = False
    consistency: bool = False
    max_metadata_bytes: int = 40960
    max_batch_size: int = 100
    supports_multimodal: bool = False


@dataclass(frozen=True, slots=True)
class SearchRequest:
    """Represent search request."""
    query_text: str = ""
    query_vector: list[float] | None = None
    top_k: int = 10
    filters: dict[str, Any] = field(default_factory=dict)
    include_metadata: bool = True
    include_vectors: bool = False
    score_threshold: float | None = None
    query_plan_hints: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SearchResult:
    """Represent search result."""
    id: str
    score: float
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SearchResponse:
    """Represent search response."""
    results: list[SearchResult] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class Job:
    """Represent job."""
    job_id: str
    job_type: str
    status: str
    collection: str = ""
    tenant_id: str = ""
    progress: float = 0.0
    error: str = ""
    correlation_id: str = ""
