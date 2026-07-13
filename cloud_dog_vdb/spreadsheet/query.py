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

"""Structured extraction query API (requirements section 5.17).

Queries run over an already-extracted :class:`WorkbookExtraction` (the canonical
model) without re-parsing the workbook. Results carry typed values, source
coordinates, schema, provenance, and extraction warnings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from cloud_dog_vdb.spreadsheet.coords import parse_range_ref
from cloud_dog_vdb.spreadsheet.model import TableNode, WorkbookExtraction


@dataclass
class ExtractionQuery:
    """A structured extraction query (section 5.17)."""

    sheet: str | None = None
    table: str | None = None
    named_range: str | None = None
    range: str | None = None
    columns: list[str] | None = None
    rows: dict[str, int] | None = None
    where: dict[str, Any] | None = None
    object_type: str | None = None


@dataclass
class ExtractionQueryResult:
    """Common result structure for a structured extraction query (section 5.17)."""

    object_type: str
    columns: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    schema: dict[str, str] = field(default_factory=dict)
    coordinates: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def run_query(extraction: WorkbookExtraction, query: ExtractionQuery) -> list[ExtractionQueryResult]:
    """Execute ``query`` against ``extraction`` and return matching results (section 5.17)."""
    if query.named_range is not None:
        return _named_range_results(extraction, query)
    if query.object_type == "pivot":
        return _pivot_results(extraction, query)
    if query.object_type in ("sheet", "named_range", "formula"):
        return _object_summary_results(extraction, query)

    tables = _select_tables(extraction, query)
    return [_table_result(extraction, table, query) for table in tables]


def _select_tables(extraction: WorkbookExtraction, query: ExtractionQuery) -> list[TableNode]:
    sheet_id_by_name = {s.sheet_name: s.sheet_id for s in extraction.sheets}
    wanted_sheet_id = sheet_id_by_name.get(query.sheet) if query.sheet else None
    bounds = None
    if query.range:
        try:
            bounds = parse_range_ref(query.range)
        except ValueError:
            bounds = None

    selected: list[TableNode] = []
    for table in extraction.tables:
        if query.table and table.table_name != query.table:
            continue
        if wanted_sheet_id is not None and table.sheet_id != wanted_sheet_id:
            continue
        if bounds is not None and not _ranges_intersect(bounds, table.range_ref):
            continue
        selected.append(table)
    return selected


def _table_result(extraction: WorkbookExtraction, table: TableNode, query: ExtractionQuery) -> ExtractionQueryResult:
    warnings = list(table.warnings)
    all_columns = list(table.normalised_column_names)
    schema = {c.column_name: c.data_type_hint for c in table.columns}

    selected_columns = all_columns
    if query.columns:
        selected_columns = [c for c in query.columns if c in all_columns]
        missing = [c for c in query.columns if c not in all_columns]
        if missing:
            warnings.append(f"columns not found: {', '.join(missing)}")
    col_index = {name: idx for idx, name in enumerate(all_columns)}

    body_start_abs = table.header_row_index + 2  # 1-based row number of first data row
    rows: list[list[Any]] = []
    for offset, row in enumerate(table.data_rows):
        absolute_row = body_start_abs + offset
        if query.rows and not (
            query.rows.get("start", absolute_row) <= absolute_row <= query.rows.get("end", absolute_row)
        ):
            continue
        if query.where and not _matches_where(row, col_index, query.where):
            continue
        rows.append([row[col_index[name]] if col_index[name] < len(row) else None for name in selected_columns])

    return ExtractionQueryResult(
        object_type="table",
        columns=selected_columns,
        rows=rows,
        schema={name: schema.get(name, "unknown") for name in selected_columns},
        coordinates={"sheet": _sheet_name(extraction, table.sheet_id), "range_ref": table.range_ref},
        provenance={
            "workbook_id": extraction.workbook.workbook_id,
            "source_uri": extraction.workbook.source_uri,
            "table_name": table.table_name,
            "table_kind": table.table_kind,
        },
        warnings=warnings,
    )


def _matches_where(row: list[Any], col_index: dict[str, int], where: dict[str, Any]) -> bool:
    for key, expected in where.items():
        idx = col_index.get(key)
        if idx is None or idx >= len(row):
            return False
        if str(row[idx]) != str(expected):
            return False
    return True


def _named_range_results(extraction: WorkbookExtraction, query: ExtractionQuery) -> list[ExtractionQueryResult]:
    results: list[ExtractionQueryResult] = []
    for named in extraction.named_ranges:
        if query.named_range and named.name != query.named_range:
            continue
        results.append(
            ExtractionQueryResult(
                object_type="named_range",
                coordinates={"refers_to": named.refers_to, "scope": named.scope},
                provenance={"workbook_id": extraction.workbook.workbook_id, "name": named.name},
            )
        )
    return results


def _pivot_results(extraction: WorkbookExtraction, query: ExtractionQuery) -> list[ExtractionQueryResult]:
    results: list[ExtractionQueryResult] = []
    for pivot in extraction.pivots:
        results.append(
            ExtractionQueryResult(
                object_type="pivot",
                columns=list(pivot.value_fields),
                schema={"row_fields": ",".join(pivot.row_fields), "value_fields": ",".join(pivot.value_fields)},
                coordinates={"sheet": _sheet_name(extraction, pivot.sheet_id), "range_ref": pivot.range_ref},
                provenance={"workbook_id": extraction.workbook.workbook_id, "pivot_name": pivot.pivot_name},
            )
        )
    return results


def _object_summary_results(extraction: WorkbookExtraction, query: ExtractionQuery) -> list[ExtractionQueryResult]:
    records = [r for r in extraction.searchable_records if r.object_type == query.object_type]
    return [
        ExtractionQueryResult(
            object_type=str(query.object_type),
            coordinates={"range_ref": r.metadata.get("range_ref", ""), "sheet": r.metadata.get("sheet_name", "")},
            provenance={"workbook_id": extraction.workbook.workbook_id, "title": r.title, "object_key": r.delete_key},
        )
        for r in records
    ]


def _ranges_intersect(bounds: tuple[int, int, int, int], range_ref: str) -> bool:
    try:
        r0, c0, r1, c1 = parse_range_ref(range_ref)
    except ValueError:
        return False
    q0, qc0, q1, qc1 = bounds
    return q0 <= r1 and r0 <= q1 and qc0 <= c1 and c0 <= qc1


def _sheet_name(extraction: WorkbookExtraction, sheet_id: str) -> str:
    for sheet in extraction.sheets:
        if sheet.sheet_id == sheet_id:
            return sheet.sheet_name
    return ""
