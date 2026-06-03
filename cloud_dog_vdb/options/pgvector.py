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


def pgvector_options(overrides: dict | None = None) -> dict:
    """Handle pgvector options."""
    out = {
        "index_type": "ivfflat",
        "probes": 10,
        "lists": 100,
    }
    if overrides:
        out.update(overrides)
    out["probes"] = max(1, int(out.get("probes", 10)))
    return out
