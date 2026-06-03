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

from cloud_dog_vdb.ingestion.parse.providers.marker_mcp import _coerce_marker_toc_blocks


def test_ut3_3_marker_toc_entries_become_heading_blocks() -> None:
    payload = {
        "success": True,
        "output": "# Report",
        "metadata": {
            "table_of_contents": [
                {"title": "Global Overview", "level": 1, "page_id": 0},
                {"title": "Regional Trends", "level": 2, "page_id": 4},
            ]
        },
    }

    blocks = _coerce_marker_toc_blocks(payload)
    assert len(blocks) == 2
    assert blocks[0].kind == "heading"
    assert blocks[0].text == "# Global Overview"
    assert blocks[0].page == 0
    assert blocks[1].text == "## Regional Trends"
    assert blocks[1].page == 4
