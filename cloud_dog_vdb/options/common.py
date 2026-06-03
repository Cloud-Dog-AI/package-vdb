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

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CommonIndexingOptions:
    """Represent common indexing options."""

    dimension: int = 1024
    distance_metric: str = "cosine"
    index_type: str = "hnsw"


@dataclass(frozen=True, slots=True)
class CommonSearchOptions:
    """Represent common search options."""

    top_k: int = 10
    score_threshold: float = 0.0
    hybrid_enabled: bool = False
