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

SUPPORTED_TABLE_POLICIES = {
    "table_as_text",
    "table_as_markdown",
    "table_as_html",
    "table_as_json",
    "table_dual",
}


def normalise_table_policy(policy: str) -> str:
    """Handle normalise table policy."""
    value = str(policy or "table_as_markdown").strip().lower()
    if value not in SUPPORTED_TABLE_POLICIES:
        return "table_as_markdown"
    return value
