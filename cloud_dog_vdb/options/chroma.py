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


def chroma_options(overrides: dict | None = None) -> dict:
    """Handle chroma options."""
    out = {
        "hnsw_space": "cosine",
        "hnsw_m": 16,
        "hnsw_ef_construction": 100,
    }
    if overrides:
        out.update(overrides)
    out["hnsw_m"] = max(4, int(out.get("hnsw_m", 16)))
    return out
