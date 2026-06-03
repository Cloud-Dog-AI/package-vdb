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

import hashlib


def deterministic_vector(text: str, dim: int) -> list[float]:
    """Build a stable pseudo-embedding from text for backend-vector operations."""
    if dim <= 0:
        raise ValueError("Vector dimension must be > 0")
    out: list[float] = []
    material = text.encode("utf-8")
    counter = 0
    while len(out) < dim:
        digest = hashlib.sha256(material + counter.to_bytes(4, "big", signed=False)).digest()
        for i in range(0, len(digest), 4):
            if len(out) >= dim:
                break
            raw = int.from_bytes(digest[i : i + 4], "big", signed=False)
            out.append((raw / 2147483648.0) - 1.0)
        counter += 1
    return out
