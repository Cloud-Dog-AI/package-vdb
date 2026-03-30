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

from cloud_dog_vdb.ingestion.parse.ir import TableBlock


def table_json_payload(table: TableBlock, *, shape: str = "records") -> dict:
    """Handle table json payload."""
    if shape == "rows_cols":
        return {
            "headers": list(table.headers),
            "rows": [list(row) for row in table.rows],
            "page": table.page,
            "locator": table.locator,
        }
    records: list[dict[str, str]] = []
    for row in table.rows:
        row_dict: dict[str, str] = {}
        for index, value in enumerate(row):
            key = table.headers[index] if index < len(table.headers) and table.headers[index] else f"col_{index}"
            row_dict[key] = value
        records.append(row_dict)
    return {
        "records": records,
        "page": table.page,
        "locator": table.locator,
    }
