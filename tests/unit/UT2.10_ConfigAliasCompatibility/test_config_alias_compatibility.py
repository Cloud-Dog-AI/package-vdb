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


def _normalise_aliases(config: dict[str, object]) -> dict[str, object]:
    out = dict(config)
    if "chunk_size" in out and "size" not in out:
        out["size"] = out["chunk_size"]
    if "chunk_overlap" in out and "overlap" not in out:
        out["overlap"] = out["chunk_overlap"]
    return out


def test_ut2_10_aliases_map_to_new_keys() -> None:
    normalised = _normalise_aliases({"chunk_size": 512, "chunk_overlap": 64})
    assert normalised["size"] == 512
    assert normalised["overlap"] == 64
