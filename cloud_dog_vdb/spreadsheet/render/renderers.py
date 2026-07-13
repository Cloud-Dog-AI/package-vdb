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

"""Per-object-type text renderers (requirements section 9.1).

Each renderer produces the searchable ``text`` for one object type. Text is
plain, deterministic, and preserves identifiers in plaintext so that lexical /
hybrid retrieval (section 16) can match exact business terms.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from cloud_dog_vdb.spreadsheet.model import (
    ColumnNode,
    FormulaNode,
    NamedRangeNode,
    PivotNode,
    RangeNode,
    RowBatchNode,
    SheetNode,
    TableNode,
    WorkbookNode,
)


def _value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, _dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, _dt.date):
        return value.isoformat()
    return str(value).strip()


def _lines(*parts: str) -> str:
    return "\n".join(part for part in parts if part)


def render_workbook(workbook: WorkbookNode, sheets: list[SheetNode]) -> str:
    """Workbook summary: name, sheet inventory, key themes (section 9.1)."""
    inventory = ", ".join(f"{s.sheet_name}" for s in sheets) or "(no sheets)"
    table_total = sum(s.table_count for s in sheets)
    parts = [
        f"Workbook: {workbook.file_name}",
        f"Format: {workbook.file_format}; sheets: {workbook.sheet_count}; tables: {table_total}",
        f"Sheet inventory: {inventory}",
    ]
    if workbook.named_ranges:
        parts.append(f"Named ranges: {', '.join(workbook.named_ranges)}")
    if workbook.properties.get("title"):
        parts.append(f"Title: {workbook.properties['title']}")
    if workbook.properties.get("subject"):
        parts.append(f"Subject: {workbook.properties['subject']}")
    return _lines(*parts)


def render_sheet(sheet: SheetNode, tables: list[TableNode], ranges: list[RangeNode]) -> str:
    """Sheet summary: name, tables present, ranges, notable columns (section 9.1)."""
    table_names = ", ".join(t.table_name for t in tables) or "(none)"
    notable_columns: list[str] = []
    for table in tables:
        notable_columns.extend(c.column_name for c in table.columns)
    columns_text = ", ".join(dict.fromkeys(notable_columns)) if notable_columns else "(none)"
    parts = [
        f"Sheet: {sheet.sheet_name} (index {sheet.sheet_index}, {sheet.visibility})",
        f"Used range: {sheet.used_range}" if sheet.used_range else "",
        f"Tables: {table_names}",
        f"Detected ranges: {len(ranges)}; formulas: {sheet.formula_count}",
        f"Notable columns: {columns_text}",
    ]
    return _lines(*parts)


def render_table(table: TableNode, sheet: SheetNode, workbook: WorkbookNode) -> str:
    """Table summary: provenance, schema, context, representative values (section 9.1)."""
    schema = ", ".join(table.normalised_column_names) or "(unknown schema)"
    parts = [
        f"Table: {table.table_name} ({table.table_kind})",
        f"Workbook: {workbook.file_name}; sheet: {sheet.sheet_name}; range: {table.range_ref}",
    ]
    if table.display_name and table.display_name != table.table_name:
        parts.append(f"Display name: {table.display_name}")
    if table.context_text:
        parts.append(f"Context: {table.context_text}")
    parts.append(f"Schema ({table.column_count} columns): {schema}")
    parts.append(f"Rows: {table.row_count}; totals row: {'yes' if table.totals_row_present else 'no'}")
    samples = _representative_values(table)
    if samples:
        parts.append(f"Representative values: {samples}")
    if table.warnings:
        parts.append(f"Warnings: {', '.join(table.warnings)}")
    return _lines(*parts)


def _representative_values(table: TableNode) -> str:
    pieces: list[str] = []
    for column in table.columns:
        if column.sample_values:
            pieces.append(f"{column.column_name}=[{', '.join(column.sample_values[:3])}]")
    return "; ".join(pieces)


def render_column(column: ColumnNode, table: TableNode) -> str:
    """Column description: semantics, type, examples, relationship (section 9.1)."""
    examples = ", ".join(column.sample_values) if column.sample_values else "(none)"
    parts = [
        f"Column: {column.column_name} (in table {table.table_name})",
        f"Position: {column.column_letter or column.column_index}; type: {column.data_type_hint}",
        f"Examples: {examples}",
        f"Distinct values (est.): {column.distinct_estimate}; null ratio: {column.null_ratio}",
    ]
    if column.original_label and column.original_label != column.column_name:
        parts.append(f"Original label: {column.original_label}")
    if column.semantic_description:
        parts.append(column.semantic_description)
    return _lines(*parts)


def render_row_batch(batch: RowBatchNode, table: TableNode, header: list[str], rows: list[list[Any]]) -> str:
    """Row batch: row values in key-value form, identifiers in plaintext (section 9.1)."""
    parts = [f"Rows {batch.row_start}-{batch.row_end} of table {table.table_name}:"]
    for offset, row in enumerate(rows):
        pairs = []
        for idx, name in enumerate(header):
            cell = _value(row[idx]) if idx < len(row) else ""
            if cell:
                pairs.append(f"{name}={cell}")
        parts.append(f"[row {batch.row_start + offset}] " + "; ".join(pairs))
    return _lines(*parts)


def render_range(range_node: RangeNode, sheet: SheetNode) -> str:
    """Range summary: sheet, native ref, inferred purpose, context (section 9.1)."""
    parts = [
        f"Range: {range_node.native_ref or range_node.range_ref} on sheet {sheet.sheet_name}",
        f"Kind: {range_node.range_kind}; classification: {range_node.classification}",
    ]
    if range_node.range_name:
        parts.append(f"Named range: {range_node.range_name}")
    if range_node.context_text:
        parts.append(f"Context: {range_node.context_text}")
    return _lines(*parts)


def render_formula(formula: FormulaNode) -> str:
    """Formula block: cell, formula text, meaning (section 9.1)."""
    parts = [
        f"Formula at {formula.cell_ref}: {formula.formula_text}",
        f"Kind: {formula.formula_kind}",
    ]
    if formula.display_value:
        parts.append(f"Value: {formula.display_value}")
    if formula.semantic_description:
        parts.append(formula.semantic_description)
    return _lines(*parts)


def render_pivot(pivot: PivotNode) -> str:
    """Pivot summary: dimensions, measures, filters, source, output (section 9.1)."""
    parts = [
        f"Pivot table: {pivot.pivot_name}",
        f"Source: {pivot.source_ref}" if pivot.source_ref else "",
        f"Row fields: {', '.join(pivot.row_fields)}" if pivot.row_fields else "",
        f"Column fields: {', '.join(pivot.column_fields)}" if pivot.column_fields else "",
        f"Value fields: {', '.join(pivot.value_fields)}" if pivot.value_fields else "",
        f"Filters: {', '.join(pivot.filter_fields)}" if pivot.filter_fields else "",
        f"Output range: {pivot.range_ref}" if pivot.range_ref else "",
        f"Extraction: {pivot.extraction_completeness}",
    ]
    return _lines(*parts)


def render_named_range(named_range: NamedRangeNode) -> str:
    """Named range summary (section 9.1)."""
    return _lines(
        f"Named range: {named_range.name} (scope: {named_range.scope})",
        f"Refers to: {named_range.refers_to}",
    )
