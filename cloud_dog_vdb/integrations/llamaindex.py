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

from typing import Any


def to_llamaindex_record(record: dict) -> dict:
    """Handle to llamaindex record."""
    return {
        "text": record.get("content", ""),
        "metadata": dict(record.get("metadata", {})),
    }


def from_llamaindex_node(node: Any) -> dict:
    """Handle from llamaindex node."""
    text = getattr(node, "text", "")
    metadata = getattr(node, "metadata", {}) or {}
    return {"content": str(text), "metadata": dict(metadata)}
