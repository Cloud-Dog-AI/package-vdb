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

import json

from cloud_dog_vdb.ingestion.parse.ir import TableBlock
from cloud_dog_vdb.ingestion.table.policy import normalise_table_policy
from cloud_dog_vdb.ingestion.table.schema import table_json_payload


def _render_text(table: TableBlock) -> str:
    lines: list[str] = []
    if table.headers:
        lines.append("\t".join(table.headers))
    lines.extend("\t".join(row) for row in table.rows)
    return "\n".join(lines).strip()


def _render_markdown(table: TableBlock) -> str:
    header = (
        list(table.headers) if table.headers else [f"col_{i}" for i in range(len(table.rows[0]) if table.rows else 0)]
    )
    sep = ["---"] * len(header)
    lines = ["| " + " | ".join(header) + " |", "| " + " | ".join(sep) + " |"]
    for row in table.rows:
        values = row + [""] * (len(header) - len(row))
        lines.append("| " + " | ".join(values[: len(header)]) + " |")
    return "\n".join(lines).strip()


def _render_html(table: TableBlock) -> str:
    parts = ["<table>"]
    if table.headers:
        parts.append("  <thead><tr>" + "".join(f"<th>{h}</th>" for h in table.headers) + "</tr></thead>")
    parts.append("  <tbody>")
    for row in table.rows:
        parts.append("    <tr>" + "".join(f"<td>{v}</td>" for v in row) + "</tr>")
    parts.append("  </tbody>")
    parts.append("</table>")
    return "\n".join(parts)


def render_table_block(table: TableBlock, *, policy: str, json_shape: str = "records") -> str:
    """Handle render table block."""
    mode = normalise_table_policy(policy)
    if mode == "table_as_text":
        return _render_text(table)
    if mode == "table_as_html":
        return _render_html(table)
    if mode == "table_as_json":
        return json.dumps(table_json_payload(table, shape=json_shape), sort_keys=True)
    if mode == "table_dual":
        markdown = _render_markdown(table)
        payload = json.dumps(table_json_payload(table, shape=json_shape), sort_keys=True)
        return f"{markdown}\n\n```json\n{payload}\n```"
    return _render_markdown(table)


def render_tables(tables: list[TableBlock], *, policy: str, json_shape: str = "records") -> list[str]:
    """Handle render tables."""
    return [render_table_block(table, policy=policy, json_shape=json_shape) for table in tables]
