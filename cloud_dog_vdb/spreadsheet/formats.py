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

"""Common extracted-data format exports (requirements section 5.16).

Provides canonical JSON for the whole workbook model, JSON Lines for searchable
records, and CSV / Parquet exports for individual extracted tables. Original
display values and normalised typed values are preserved where present.
"""

from __future__ import annotations

import csv
import datetime as _dt
import io
import json
from dataclasses import asdict
from typing import Any

from cloud_dog_vdb.spreadsheet.model import TableNode, WorkbookExtraction


def _json_safe(value: Any) -> Any:
    if isinstance(value, _dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, _dt.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def workbook_to_json(extraction: WorkbookExtraction) -> dict[str, Any]:
    """Return a canonical, JSON-safe dict of the full workbook model (section 5.16)."""
    return {
        "workbook": _json_safe(asdict(extraction.workbook)),
        "sheets": [_json_safe(asdict(s)) for s in extraction.sheets],
        "tables": [_json_safe(asdict(t)) for t in extraction.tables],
        "ranges": [_json_safe(asdict(r)) for r in extraction.ranges],
        "columns": [_json_safe(asdict(c)) for c in extraction.columns],
        "formulas": [_json_safe(asdict(f)) for f in extraction.formulas],
        "pivots": [_json_safe(asdict(p)) for p in extraction.pivots],
        "named_ranges": [_json_safe(asdict(n)) for n in extraction.named_ranges],
        "manifest": [_json_safe(asdict(m)) for m in extraction.manifest],
        "stats": dict(extraction.stats),
    }


def workbook_to_json_str(extraction: WorkbookExtraction, *, indent: int | None = None) -> str:
    """Serialise the canonical workbook JSON to a string."""
    return json.dumps(workbook_to_json(extraction), ensure_ascii=False, indent=indent)


def records_to_jsonl(extraction: WorkbookExtraction) -> str:
    """Return the searchable records as JSON Lines (section 5.16)."""
    lines: list[str] = []
    for record in extraction.searchable_records:
        payload = {
            "record_id": record.record_id,
            "object_type": record.object_type,
            "source_type": record.source_type,
            "title": record.title,
            "text": record.text,
            "language": record.language,
            "keyword_text": record.keyword_text,
            "metadata": _json_safe(record.metadata),
        }
        lines.append(json.dumps(payload, ensure_ascii=False))
    return "\n".join(lines)


def table_to_csv(table: TableNode) -> str:
    """Return a CSV export of one extracted table (header + typed rows, section 5.16)."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(table.normalised_column_names)
    for row in table.data_rows:
        writer.writerow(["" if v is None else _csv_cell(v) for v in row])
    return buffer.getvalue()


def _csv_cell(value: Any) -> Any:
    if isinstance(value, _dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, _dt.date):
        return value.isoformat()
    return value


def table_to_parquet(table: TableNode) -> bytes:
    """Return a Parquet snapshot of one extracted table (section 5.16, optional).

    Requires the ``parquet`` extra (pyarrow). Raises a clear error if unavailable.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise RuntimeError("Parquet export requires the 'parquet' extra (pyarrow)") from exc

    columns = table.normalised_column_names
    data = {name: [] for name in columns}
    for row in table.data_rows:
        for idx, name in enumerate(columns):
            data[name].append(row[idx] if idx < len(row) else None)
    arrow_table = pa.table({name: pa.array(values) for name, values in data.items()})
    buffer = io.BytesIO()
    pq.write_table(arrow_table, buffer)
    return buffer.getvalue()
