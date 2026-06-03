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


def verify_ids(ids: list[str]) -> bool:
    """Handle verify ids."""
    if not ids:
        return False
    clean = [i for i in ids if isinstance(i, str) and i.strip()]
    if len(clean) != len(ids):
        return False
    return len(set(clean)) == len(clean)


def verify_minimum(ids: list[str], expected_min: int) -> bool:
    """Handle verify minimum."""
    return verify_ids(ids) and len(ids) >= max(0, expected_min)
