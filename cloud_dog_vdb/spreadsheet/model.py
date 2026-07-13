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

"""Canonical workbook object model (requirements section 8) and the searchable
record model (section 9).

The model decouples parsing from backend indexing: parsers produce the canonical
nodes, renderers turn them into ``SearchableRecord`` text, and the pipeline emits
backend-neutral :class:`cloud_dog_vdb.domain.models.Record` objects. Nodes are
mutable because the pipeline builds them incrementally across stages.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: Source type tag carried on every emitted record (section 9).
SOURCE_TYPE_EXCEL = "excel"

#: Object types emitted to vector backends (section 5.10 / 9).
OBJECT_TYPES = (
    "workbook",
    "sheet",
    "table",
    "range",
    "column",
    "row_batch",
    "formula",
    "pivot",
    "named_range",
)

#: Table classifications (section 8 TableNode.table_kind / section 12.2).
TABLE_KINDS = ("formal", "inferred", "pivot_output", "report_grid")

#: Region classification outcomes (section 12.2).
REGION_CLASSES = ("dataset", "report_grid", "pivot_output", "notes", "decorative")

#: Region classes that proceed to chunking/indexing (section 12.2).
INDEXABLE_REGION_CLASSES = ("dataset", "report_grid", "pivot_output")


@dataclass
class WorkbookNode:
    """Represent a source workbook (section 8 WorkbookNode)."""

    workbook_id: str
    source_uri: str
    file_name: str = ""
    file_hash: str = ""
    version_hash: str = ""
    file_format: str = ""
    size_bytes: int = 0
    created_at: str = ""
    modified_at: str = ""
    sheet_count: int = 0
    visibility_flags: dict[str, str] = field(default_factory=dict)
    named_ranges: list[str] = field(default_factory=list)
    properties: dict[str, Any] = field(default_factory=dict)
    language: str = ""
    locale: str = ""
    parse_status: str = "complete"
    warnings: list[str] = field(default_factory=list)
    summary_text: str = ""


@dataclass
class SheetNode:
    """Represent a worksheet (section 8 SheetNode)."""

    sheet_id: str
    workbook_id: str
    sheet_name: str
    sheet_index: int
    visibility: str = "visible"
    used_range: str = ""
    frozen_panes: str = ""
    hidden_rows: list[int] = field(default_factory=list)
    hidden_columns: list[int] = field(default_factory=list)
    inferred_region_count: int = 0
    table_count: int = 0
    formula_count: int = 0
    language: str = ""
    summary_text: str = ""


@dataclass
class ColumnNode:
    """Represent a column in a table or inferred region (section 8 ColumnNode)."""

    column_id: str
    table_id: str
    column_name: str
    column_index: int
    column_letter: str = ""
    original_label: str = ""
    data_type_hint: str = "unknown"
    sample_values: list[str] = field(default_factory=list)
    distinct_estimate: int = 0
    null_ratio: float = 0.0
    numeric_min: float | None = None
    numeric_max: float | None = None
    numeric_mean: float | None = None
    semantic_description: str = ""


@dataclass
class RowBatchNode:
    """Represent a batch of rows or a single row (section 8 RowBatchNode)."""

    row_batch_id: str
    table_id: str
    row_start: int
    row_end: int
    record_count: int = 0
    key_fields: dict[str, Any] = field(default_factory=dict)
    rendered_text: str = ""
    row_hash: str = ""


@dataclass
class TableNode:
    """Represent a formal or inferred table (section 8 TableNode)."""

    table_id: str
    workbook_id: str
    sheet_id: str
    table_name: str
    table_kind: str = "inferred"
    display_name: str = ""
    range_ref: str = ""
    source_range: str = ""
    header_row_index: int = 0
    totals_row_present: bool = False
    row_count: int = 0
    column_count: int = 0
    normalised_column_names: list[str] = field(default_factory=list)
    original_column_labels: list[str] = field(default_factory=list)
    schema_signature: str = ""
    content_signature: str = ""
    context_text: str = ""
    header_confidence: float = 0.0
    classification_confidence: float = 0.0
    extraction_status: str = "complete"
    warnings: list[str] = field(default_factory=list)
    summary_text: str = ""
    columns: list[ColumnNode] = field(default_factory=list)
    row_batches: list[RowBatchNode] = field(default_factory=list)
    #: Typed body cell values (header excluded), capped by config.max_rows_per_table.
    #: Backs the structured extraction query API (5.17) and table format exports (5.16).
    data_rows: list[list[Any]] = field(default_factory=list)


@dataclass
class RangeNode:
    """Represent a generic extracted range (section 8 RangeNode)."""

    range_id: str
    workbook_id: str
    sheet_id: str
    range_kind: str = "inferred_range"
    range_name: str | None = None
    range_ref: str = ""
    native_ref: str = ""
    start_row: int = 0
    end_row: int = 0
    start_column: int = 0
    end_column: int = 0
    context_text: str = ""
    classification: str = "dataset"
    classification_confidence: float = 0.0
    summary_text: str = ""


@dataclass
class FormulaNode:
    """Represent a formula cell or grouped formula block (section 8 FormulaNode)."""

    formula_id: str
    sheet_id: str
    cell_ref: str
    formula_text: str
    table_id: str | None = None
    display_value: str = ""
    formula_kind: str = "cell"
    semantic_description: str = ""


@dataclass
class PivotNode:
    """Represent a pivot artefact (section 8 PivotNode)."""

    pivot_id: str
    sheet_id: str
    pivot_name: str
    source_ref: str = ""
    row_fields: list[str] = field(default_factory=list)
    column_fields: list[str] = field(default_factory=list)
    value_fields: list[str] = field(default_factory=list)
    filter_fields: list[str] = field(default_factory=list)
    range_ref: str = ""
    summary_text: str = ""
    extraction_completeness: str = "complete"


@dataclass
class NamedRangeNode:
    """Represent a workbook- or sheet-level named range (section 8 NamedRangeNode)."""

    named_range_id: str
    workbook_id: str
    name: str
    scope: str = "workbook"
    refers_to: str = ""
    summary_text: str = ""


@dataclass
class SearchableRecord:
    """Common searchable record sent to vector backends (section 9).

    ``keyword_text`` carries lexical cues (workbook/sheet/table/column names and
    identifiers) for hybrid retrieval (section 16); ``delete_key`` supports
    object-aware deletion on re-index (section 5.18).
    """

    record_id: str
    object_type: str
    title: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    source_type: str = SOURCE_TYPE_EXCEL
    language: str = ""
    locale: str = ""
    embedding_input_profile: str = "default"
    keyword_text: str = ""
    source_hash: str = ""
    permissions: list[str] = field(default_factory=list)
    delete_key: str = ""


@dataclass
class ObjectManifestEntry:
    """One object's identity + hash, used for incremental refresh (section 5.18)."""

    object_key: str
    object_type: str
    object_hash: str
    parent_object_key: str = ""
    sheet_name: str = ""
    table_name: str = ""
    range_ref: str = ""


@dataclass
class WorkbookExtraction:
    """Full canonical extraction of one workbook.

    This is the pure, backend-neutral output of
    :func:`cloud_dog_vdb.spreadsheet.pipeline.extract_workbook`. The caller
    (e.g. the index-retriever service) persists ``manifest`` into the SQL
    control plane and upserts ``records`` via the vector backend client.
    """

    workbook: WorkbookNode
    sheets: list[SheetNode] = field(default_factory=list)
    tables: list[TableNode] = field(default_factory=list)
    ranges: list[RangeNode] = field(default_factory=list)
    columns: list[ColumnNode] = field(default_factory=list)
    row_batches: list[RowBatchNode] = field(default_factory=list)
    formulas: list[FormulaNode] = field(default_factory=list)
    pivots: list[PivotNode] = field(default_factory=list)
    named_ranges: list[NamedRangeNode] = field(default_factory=list)
    searchable_records: list[SearchableRecord] = field(default_factory=list)
    manifest: list[ObjectManifestEntry] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)

    def object_count_by_type(self) -> dict[str, int]:
        """Return a count of emitted searchable records per object type."""
        counts: dict[str, int] = {}
        for record in self.searchable_records:
            counts[record.object_type] = counts.get(record.object_type, 0) + 1
        return counts
