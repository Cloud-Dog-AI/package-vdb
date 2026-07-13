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

"""Spreadsheet ingestion pipeline (requirements section 10).

``extract_workbook`` runs stages 2-6 (parse, region detection, normalisation,
profiling/summarisation, search-record generation) and returns a pure
:class:`WorkbookExtraction`. The hosting service performs stages 1, 7, 8 and 9
(source acquisition + SQL registration, embedding, backend upsert, finalisation)
using the existing ``cloud_dog_vdb`` client — this module never touches a
backend, a database, or the environment.
"""

from __future__ import annotations

from typing import Any

from cloud_dog_vdb.domain.models import Record
from cloud_dog_vdb.spreadsheet import coords, ids
from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.detect.classify import classify_region
from cloud_dog_vdb.spreadsheet.detect.profile import profile_column
from cloud_dog_vdb.spreadsheet.detect.regions import detect_regions
from cloud_dog_vdb.spreadsheet.i18n import detect_language
from cloud_dog_vdb.spreadsheet.sensitivity import REDACTION, SensitivityPolicy
from cloud_dog_vdb.spreadsheet.model import (
    INDEXABLE_REGION_CLASSES,
    ColumnNode,
    FormulaNode,
    NamedRangeNode,
    ObjectManifestEntry,
    PivotNode,
    RowBatchNode,
    SearchableRecord,
    SheetNode,
    TableNode,
    WorkbookExtraction,
    WorkbookNode,
)
from cloud_dog_vdb.spreadsheet.parser.base import (
    RawSheet,
    RawWorkbook,
    UnsupportedFormatError,
    detect_format,
    get_parser_for,
)
from cloud_dog_vdb.spreadsheet.render import renderers


class WorkbookTooLargeError(ValueError):
    """Raised when a workbook exceeds the configured maximum size (section 5.15)."""

    def __init__(self, size: int, limit: int) -> None:
        super().__init__(f"workbook size {size} exceeds limit {limit}")
        self.size = size
        self.limit = limit


def extract_workbook(
    data: bytes,
    *,
    file_name: str,
    source_uri: str = "",
    config: SpreadsheetConfig | None = None,
    file_format: str = "",
    permissions: list[str] | None = None,
) -> WorkbookExtraction:
    """Extract a workbook into canonical nodes + searchable records (section 10).

    Raises :class:`WorkbookTooLargeError` and :class:`UnsupportedFormatError`.
    Parse-time failures degrade gracefully into a ``failed`` extraction carrying
    warnings so the caller can record them (section 5.1 / 17 partial recovery).
    """
    config = config or SpreadsheetConfig()
    config.validate()
    permissions = permissions or []

    if len(data) > config.max_workbook_bytes:
        raise WorkbookTooLargeError(len(data), config.max_workbook_bytes)

    fmt = (file_format or detect_format(file_name, data)).lower().lstrip(".")
    if not fmt:
        raise UnsupportedFormatError(file_format or file_name)
    parser = get_parser_for(fmt)

    file_hash = ids.bytes_hash(data)
    source = source_uri or file_name
    # Object identity keys off the stable source, NOT the file bytes, so re-saving
    # a workbook (new bytes, same content) yields matching object keys and lets the
    # content-based object_hash detect what actually changed (sections 5.14, 5.18).
    workbook_id = ids.stable_id(source)

    try:
        raw = parser.parse(data, file_name=file_name, config=config)
    except (WorkbookTooLargeError, UnsupportedFormatError):
        raise
    except Exception as exc:  # graceful degradation (section 5.1, 17)
        return _failed_extraction(workbook_id, source, file_name, fmt, file_hash, len(data), exc)

    return _build_extraction(raw, workbook_id, source, file_hash, len(data), config, permissions)


def _failed_extraction(
    workbook_id: str,
    source_uri: str,
    file_name: str,
    fmt: str,
    file_hash: str,
    size_bytes: int,
    exc: Exception,
) -> WorkbookExtraction:
    workbook = WorkbookNode(
        workbook_id=workbook_id,
        source_uri=source_uri,
        file_name=file_name,
        file_hash=file_hash,
        version_hash=file_hash,
        file_format=fmt,
        size_bytes=size_bytes,
        parse_status="failed",
        warnings=[f"parse failed: {exc}"],
    )
    return WorkbookExtraction(workbook=workbook, warnings=list(workbook.warnings), stats={"parse_failed": 1})


def _build_extraction(
    raw: RawWorkbook,
    workbook_id: str,
    source_uri: str,
    file_hash: str,
    size_bytes: int,
    config: SpreadsheetConfig,
    permissions: list[str],
) -> WorkbookExtraction:
    version_hash = ids.stable_id(file_hash, "parser=openpyxl/odf", config.extraction_mode)
    workbook = WorkbookNode(
        workbook_id=workbook_id,
        source_uri=source_uri,
        file_name=raw.file_name,
        file_hash=file_hash,
        version_hash=version_hash,
        file_format=raw.file_format,
        size_bytes=size_bytes,
        sheet_count=len(raw.sheets),
        visibility_flags={s.name: s.visibility for s in raw.sheets},
        named_ranges=[nr.name for nr in raw.named_ranges],
        properties=dict(raw.properties),
        parse_status=raw.parse_status,
        warnings=list(raw.warnings),
    )
    if raw.has_macros:
        workbook.warnings.append("workbook contains macros; macros were not executed")

    extraction = WorkbookExtraction(workbook=workbook, warnings=list(workbook.warnings))
    policy = SensitivityPolicy.from_config(config)
    ctx = _Context(workbook=workbook, config=config, permissions=permissions, extraction=extraction, policy=policy)

    for raw_sheet in raw.sheets:
        if raw_sheet.visibility != "visible" and not config.include_hidden_sheets:
            continue
        if policy.sheet_excluded(raw_sheet.name):
            workbook.warnings.append(f"sheet {raw_sheet.name} excluded by sensitivity policy")
            continue
        _process_sheet(ctx, raw_sheet)

    _process_named_ranges(ctx, raw)

    # Workbook language is the dominant sheet language (section 5.19).
    if config.detect_language:
        langs = [s.language for s in extraction.sheets if s.language and s.language != "und"]
        workbook.language = max(set(langs), key=langs.count) if langs else "und"
    ctx.current_language = workbook.language

    # Workbook summary record (built last so the sheet inventory is complete).
    workbook.summary_text = renderers.render_workbook(workbook, extraction.sheets)
    wb_key = f"wb:{workbook_id}"
    ctx.emit(
        record_id=wb_key,
        object_type="workbook",
        title=workbook.file_name,
        text=workbook.summary_text,
        object_key=wb_key,
        keyword_text=" ".join([workbook.file_name, *workbook.named_ranges]),
        extra={},
    )

    extraction.stats.update(extraction.object_count_by_type())
    extraction.stats["sheets"] = len(extraction.sheets)
    extraction.stats["tables"] = len(extraction.tables)
    extraction.stats["records"] = len(extraction.searchable_records)
    extraction.stats["warnings"] = len(workbook.warnings)
    extraction.stats["redactions"] = ctx.redaction_count
    return extraction


class _Context:
    """Mutable accumulator threaded through pipeline stages."""

    def __init__(
        self,
        workbook: WorkbookNode,
        config: SpreadsheetConfig,
        permissions: list[str],
        extraction: WorkbookExtraction,
        policy: SensitivityPolicy,
    ) -> None:
        self.workbook = workbook
        self.config = config
        self.permissions = permissions
        self.extraction = extraction
        self.policy = policy
        self.current_language = ""
        self.redaction_count = 0

    def emit(
        self,
        *,
        record_id: str,
        object_type: str,
        title: str,
        text: str,
        object_key: str,
        keyword_text: str,
        extra: dict[str, Any],
        parent_object_key: str = "",
        sheet_name: str = "",
        table_name: str = "",
        range_ref: str = "",
    ) -> None:
        metadata: dict[str, Any] = {
            "object_type": object_type,
            "source_type": "excel",
            "workbook_id": self.workbook.workbook_id,
            "source_uri": self.workbook.source_uri,
            "file_name": self.workbook.file_name,
            "file_format": self.workbook.file_format,
            "file_hash": self.workbook.file_hash,
            "version_hash": self.workbook.version_hash,
            "object_key": object_key,
            "parent_object_key": parent_object_key,
            "sheet_name": sheet_name,
            "table_name": table_name,
            "range_ref": range_ref,
            "locale": self.workbook.locale,
        }
        language = self.current_language or self.workbook.language
        if self.policy.redact:
            text, redacted_text = self.policy.redact_text(text)
            keyword_text, redacted_kw = self.policy.redact_text(keyword_text)
            redactions = redacted_text + redacted_kw
            if redactions:
                self.redaction_count += redactions
                extra = {**extra, "redacted": True, "redaction_count": redactions}
        metadata["language"] = language
        metadata.update(extra)
        source_hash = ids.content_hash(text)
        record = SearchableRecord(
            record_id=record_id,
            object_type=object_type,
            title=title,
            text=text,
            metadata=metadata,
            language=language,
            locale=self.workbook.locale,
            embedding_input_profile=self.config.embedding_input_profile,
            keyword_text=keyword_text,
            source_hash=source_hash,
            permissions=list(self.permissions),
            delete_key=object_key,
        )
        self.extraction.searchable_records.append(record)
        self.extraction.manifest.append(
            ObjectManifestEntry(
                object_key=object_key,
                object_type=object_type,
                object_hash=source_hash,
                parent_object_key=parent_object_key,
                sheet_name=sheet_name,
                table_name=table_name,
                range_ref=range_ref,
            )
        )


def _process_sheet(ctx: _Context, raw_sheet: RawSheet) -> None:
    workbook = ctx.workbook
    sheet_id = ids.stable_id(workbook.workbook_id, "sheet", raw_sheet.index)
    sheet_key = f"wb:{workbook.workbook_id}:sheet:{raw_sheet.index}"
    sheet = SheetNode(
        sheet_id=sheet_id,
        workbook_id=workbook.workbook_id,
        sheet_name=raw_sheet.name,
        sheet_index=raw_sheet.index,
        visibility=raw_sheet.visibility,
        used_range=raw_sheet.used_range,
        frozen_panes=raw_sheet.frozen_panes,
        hidden_rows=list(raw_sheet.hidden_rows),
        hidden_columns=list(raw_sheet.hidden_columns),
        formula_count=len(raw_sheet.formulas),
    )
    if ctx.config.detect_language:
        sample = " ".join(
            [raw_sheet.name] + [str(cell) for row in raw_sheet.cells[:8] for cell in row if isinstance(cell, str)]
        )
        sheet.language = detect_language(sample)
    ctx.current_language = sheet.language
    ctx.extraction.sheets.append(sheet)

    sheet_tables: list[TableNode] = []
    formal_bounds: list[tuple[int, int, int, int]] = []

    # Formal tables first (section 5.4).
    for raw_table in raw_sheet.formal_tables:
        bounds = _safe_range(raw_table.range_ref)
        if bounds is None:
            continue
        formal_bounds.append(bounds)
        table = _build_table(
            ctx,
            raw_sheet,
            sheet,
            sheet_key,
            bounds=bounds,
            header_row_index=raw_table.header_row_index,
            table_kind="formal",
            table_name=raw_table.name or raw_table.display_name,
            display_name=raw_table.display_name,
            totals_row_present=raw_table.totals_row_present,
        )
        if table is not None:
            sheet_tables.append(table)

    # Inferred regions (section 5.5, 12) that do not overlap a formal table.
    inferred_count = 0
    for region in detect_regions(raw_sheet, ctx.config):
        classification, confidence = classify_region(raw_sheet, region, ctx.config)
        region.classification = classification
        region.classification_confidence = confidence
        if classification not in INDEXABLE_REGION_CLASSES:
            continue
        bounds = (region.start_row, region.start_col, region.end_row, region.end_col)
        if _overlaps_any(bounds, formal_bounds):
            continue
        inferred_count += 1
        kind = "report_grid" if classification == "report_grid" else "inferred"
        table = _build_table(
            ctx,
            raw_sheet,
            sheet,
            sheet_key,
            bounds=bounds,
            header_row_index=region.header_row_index,
            table_kind=kind,
            table_name=f"inferred_{raw_sheet.name}_{coords.range_ref(*bounds)}".replace(":", "_"),
            display_name="",
            totals_row_present=False,
            classification_confidence=confidence,
            header_confidence=region.header_confidence,
        )
        if table is not None:
            sheet_tables.append(table)

    sheet.table_count = len(sheet_tables)
    sheet.inferred_region_count = inferred_count

    if ctx.config.extract_formulas:
        _process_formulas(ctx, raw_sheet, sheet, sheet_key)
    if ctx.config.pivot_extraction_mode != "off":
        _process_pivots(ctx, raw_sheet, sheet, sheet_key)

    sheet.summary_text = renderers.render_sheet(sheet, sheet_tables, [])

    ctx.emit(
        record_id=sheet_key,
        object_type="sheet",
        title=raw_sheet.name,
        text=sheet.summary_text,
        object_key=sheet_key,
        parent_object_key=f"wb:{workbook.workbook_id}",
        sheet_name=raw_sheet.name,
        keyword_text=" ".join([raw_sheet.name, *(t.table_name for t in sheet_tables)]),
        extra={"sheet_index": raw_sheet.index, "visibility": raw_sheet.visibility},
    )


def _build_table(
    ctx: _Context,
    raw_sheet: RawSheet,
    sheet: SheetNode,
    sheet_key: str,
    *,
    bounds: tuple[int, int, int, int],
    header_row_index: int,
    table_kind: str,
    table_name: str,
    display_name: str,
    totals_row_present: bool,
    classification_confidence: float = 0.0,
    header_confidence: float = 0.0,
) -> TableNode | None:
    r0, c0, r1, c1 = bounds
    cells = raw_sheet.cells
    header_row_index = max(header_row_index, r0)
    header = [_cell(cells, header_row_index, c) for c in range(c0, c1 + 1)]
    normalised, originals = _normalise_columns(header)

    body_start = header_row_index + 1
    body_end = r1
    if totals_row_present and body_end > body_start:
        body_end -= 1
    body_rows_all = [[_cell(cells, r, c) for c in range(c0, c1 + 1)] for r in range(body_start, body_end + 1)]

    max_rows = min(ctx.config.max_rows_per_table, len(body_rows_all))
    truncated = max_rows < len(body_rows_all)
    body_rows = body_rows_all[:max_rows]

    workbook = ctx.workbook
    table_id = ids.stable_id(sheet.sheet_id, "table", table_name, coords.range_ref(*bounds))
    table_key = f"{sheet_key}:table:{ids.stable_id(table_name, coords.range_ref(*bounds))}"
    column_names = list(normalised)
    type_hints: list[str] = []

    table = TableNode(
        table_id=table_id,
        workbook_id=workbook.workbook_id,
        sheet_id=sheet.sheet_id,
        table_name=table_name,
        table_kind=table_kind,
        display_name=display_name,
        range_ref=coords.range_ref(*bounds),
        source_range=coords.range_ref(*bounds),
        header_row_index=header_row_index,
        totals_row_present=totals_row_present,
        row_count=len(body_rows),
        column_count=len(column_names),
        normalised_column_names=column_names,
        original_column_labels=originals,
        context_text=_context_above(cells, r0, c0, c1),
        header_confidence=header_confidence,
        classification_confidence=classification_confidence,
        extraction_status="partial" if truncated else "complete",
    )
    if truncated:
        table.warnings.append(f"row extraction truncated at {max_rows} rows")

    # Columns (section 5.6). Sensitivity-excluded columns are still profiled for
    # schema completeness, but their values and dedicated records are withheld
    # and their cells redacted in row batches (section 17.3).
    excluded_cols: set[int] = set()
    for idx, name in enumerate(column_names):
        if ctx.policy.exclude_columns and (
            ctx.policy.column_excluded(name) or ctx.policy.column_excluded(originals[idx])
        ):
            excluded_cols.add(idx)
        col_values = [row[idx] for row in body_rows if idx < len(row)]
        prof = profile_column(col_values, ctx.config)
        type_hints.append(prof.data_type_hint)
        column = ColumnNode(
            column_id=ids.stable_id(table_id, "col", idx),
            table_id=table_id,
            column_name=name,
            column_index=idx,
            column_letter=coords.column_index_to_letter(c0 + idx),
            original_label=originals[idx],
            data_type_hint=prof.data_type_hint,
            sample_values=[] if idx in excluded_cols else prof.sample_values,
            distinct_estimate=prof.distinct_estimate,
            null_ratio=prof.null_ratio,
            numeric_min=prof.numeric_min,
            numeric_max=prof.numeric_max,
            numeric_mean=prof.numeric_mean,
        )
        table.columns.append(column)

    table.schema_signature = ids.schema_signature(column_names, type_hints)
    table.content_signature = ids.content_signature(body_rows)
    table.data_rows = body_rows
    table.summary_text = renderers.render_table(table, sheet, workbook)

    ctx.extraction.tables.append(table)
    ctx.extraction.columns.extend(table.columns)

    # Table summary record (also serves as the inferred-data-region summary).
    ctx.emit(
        record_id=table_key,
        object_type="table",
        title=table.table_name,
        text=table.summary_text,
        object_key=table_key,
        parent_object_key=sheet_key,
        sheet_name=sheet.sheet_name,
        table_name=table.table_name,
        range_ref=table.range_ref,
        keyword_text=" ".join([table.table_name, *column_names]),
        extra={"table_kind": table_kind, "schema_signature": table.schema_signature},
    )

    # Column records (excluded columns are withheld from indexing, section 17.3).
    for column in table.columns:
        if column.column_index in excluded_cols:
            continue
        col_key = f"{table_key}:col:{column.column_index}"
        ctx.emit(
            record_id=col_key,
            object_type="column",
            title=f"{table.table_name}.{column.column_name}",
            text=renderers.render_column(column, table),
            object_key=col_key,
            parent_object_key=table_key,
            sheet_name=sheet.sheet_name,
            table_name=table.table_name,
            range_ref=table.range_ref,
            keyword_text=" ".join([column.column_name, column.original_label]),
            extra={
                "column_name": column.column_name,
                "column_index": column.column_index,
                "data_type_hint": column.data_type_hint,
            },
        )

    _emit_row_batches(ctx, table, table_key, sheet, column_names, body_rows, body_start, excluded_cols)
    return table


def _emit_row_batches(
    ctx: _Context,
    table: TableNode,
    table_key: str,
    sheet: SheetNode,
    header: list[str],
    body_rows: list[list[Any]],
    body_start: int,
    excluded_cols: set[int],
) -> None:
    config = ctx.config
    policy = config.row_indexing_policy
    if policy == "none" or not config.index_row_batches or not body_rows:
        return

    def _present_row(row: list[Any]) -> list[Any]:
        # Redact sensitivity-excluded columns; in key_only mode keep only column 0.
        rendered: list[Any] = []
        for col_idx, value in enumerate(row):
            if col_idx in excluded_cols:
                rendered.append(REDACTION)
            elif policy == "key_only" and col_idx != 0:
                rendered.append(None)
            else:
                rendered.append(value)
        return rendered

    # "sampled" indexes an evenly-spaced subset of rows, each as its own record.
    if policy == "sampled" and len(body_rows) > config.row_sample_size:
        step = len(body_rows) / config.row_sample_size
        indices = sorted({int(i * step) for i in range(config.row_sample_size)})
        for offset in indices:
            _emit_single_row_batch(
                ctx, table, table_key, sheet, header, body_rows[offset], body_start + offset, _present_row
            )
        return

    batch_size = 1 if len(body_rows) <= config.row_level_max_rows else config.row_batch_size
    for start in range(0, len(body_rows), batch_size):
        raw_chunk = body_rows[start : start + batch_size]
        chunk = [_present_row(row) for row in raw_chunk]
        row_start_abs = body_start + start
        row_end_abs = row_start_abs + len(chunk) - 1
        key_fields = _row_key_fields(header, chunk, excluded_cols)
        batch = RowBatchNode(
            row_batch_id=ids.stable_id(table.table_id, "rows", row_start_abs, row_end_abs),
            table_id=table.table_id,
            row_start=row_start_abs,
            row_end=row_end_abs,
            record_count=len(chunk),
            key_fields=key_fields,
            row_hash=ids.content_signature(raw_chunk),
        )
        batch.rendered_text = renderers.render_row_batch(batch, table, header, chunk)
        ctx.extraction.row_batches.append(batch)
        _emit_row_batch_record(ctx, table, table_key, sheet, batch, key_fields)


def _emit_single_row_batch(
    ctx: _Context,
    table: TableNode,
    table_key: str,
    sheet: SheetNode,
    header: list[str],
    raw_row: list[Any],
    row_abs: int,
    present: Any,
) -> None:
    chunk = [present(raw_row)]
    key_fields = _row_key_fields(header, chunk, set())
    batch = RowBatchNode(
        row_batch_id=ids.stable_id(table.table_id, "rows", row_abs, row_abs),
        table_id=table.table_id,
        row_start=row_abs,
        row_end=row_abs,
        record_count=1,
        key_fields=key_fields,
        row_hash=ids.content_signature([raw_row]),
    )
    batch.rendered_text = renderers.render_row_batch(batch, table, header, chunk)
    ctx.extraction.row_batches.append(batch)
    _emit_row_batch_record(ctx, table, table_key, sheet, batch, key_fields)


def _row_key_fields(header: list[str], chunk: list[list[Any]], excluded_cols: set[int]) -> dict[str, Any]:
    if header and chunk and chunk[0] and 0 not in excluded_cols:
        return {header[0]: _scalar(chunk[0][0])}
    return {}


def _emit_row_batch_record(
    ctx: _Context,
    table: TableNode,
    table_key: str,
    sheet: SheetNode,
    batch: RowBatchNode,
    key_fields: dict[str, Any],
) -> None:
    batch_key = f"{table_key}:rows:{batch.row_start}-{batch.row_end}"
    ctx.emit(
        record_id=batch_key,
        object_type="row_batch",
        title=f"{table.table_name} rows {batch.row_start}-{batch.row_end}",
        text=batch.rendered_text,
        object_key=batch_key,
        parent_object_key=table_key,
        sheet_name=sheet.sheet_name,
        table_name=table.table_name,
        range_ref=table.range_ref,
        keyword_text=" ".join(str(v) for v in key_fields.values()),
        extra={"row_start": batch.row_start, "row_end": batch.row_end, "record_count": batch.record_count},
    )


def _process_named_ranges(ctx: _Context, raw: RawWorkbook) -> None:
    workbook = ctx.workbook
    for raw_nr in raw.named_ranges:
        node = NamedRangeNode(
            named_range_id=ids.stable_id(workbook.workbook_id, "named_range", raw_nr.name),
            workbook_id=workbook.workbook_id,
            name=raw_nr.name,
            scope=raw_nr.scope,
            refers_to=raw_nr.refers_to,
        )
        node.summary_text = renderers.render_named_range(node)
        ctx.extraction.named_ranges.append(node)
        nr_key = f"wb:{workbook.workbook_id}:named_range:{ids.stable_id(raw_nr.name)}"
        ctx.emit(
            record_id=nr_key,
            object_type="named_range",
            title=raw_nr.name,
            text=node.summary_text,
            object_key=nr_key,
            parent_object_key=f"wb:{workbook.workbook_id}",
            keyword_text=raw_nr.name,
            extra={"refers_to": raw_nr.refers_to, "scope": raw_nr.scope},
        )


_AGG_FUNCS = ("SUM", "AVERAGE", "AVG", "COUNT", "COUNTA", "MIN", "MAX", "SUBTOTAL", "MEDIAN", "STDEV", "PRODUCT")
_LOOKUP_FUNCS = ("VLOOKUP", "HLOOKUP", "XLOOKUP", "INDEX", "MATCH", "LOOKUP", "OFFSET")
_LOGICAL_FUNCS = ("IF", "IFS", "AND", "OR", "NOT", "IFERROR", "SWITCH")
_TEXT_FUNCS = ("CONCAT", "CONCATENATE", "LEFT", "RIGHT", "MID", "TEXT", "TRIM", "UPPER", "LOWER")
_DATE_FUNCS = ("DATE", "TODAY", "NOW", "YEAR", "MONTH", "DAY", "EDATE", "DATEDIF")


def _classify_formula(text: str) -> str:
    """Classify a formula by its dominant function family (section 5.8)."""
    upper = text.upper()
    if any(f"{fn}(" in upper for fn in _AGG_FUNCS):
        return "aggregate"
    if any(f"{fn}(" in upper for fn in _LOOKUP_FUNCS):
        return "lookup"
    if any(f"{fn}(" in upper for fn in _LOGICAL_FUNCS):
        return "logical"
    if any(f"{fn}(" in upper for fn in _TEXT_FUNCS):
        return "text"
    if any(f"{fn}(" in upper for fn in _DATE_FUNCS):
        return "date"
    return "arithmetic" if any(op in text for op in "+-*/") else "reference"


def _process_formulas(ctx: _Context, raw_sheet: RawSheet, sheet: SheetNode, sheet_key: str) -> None:
    for raw_formula in raw_sheet.formulas:
        node = FormulaNode(
            formula_id=ids.stable_id(sheet.sheet_id, "formula", raw_formula.cell_ref),
            sheet_id=sheet.sheet_id,
            cell_ref=raw_formula.cell_ref,
            formula_text=raw_formula.formula_text,
            display_value=str(raw_formula.display_value or ""),
            formula_kind=_classify_formula(raw_formula.formula_text),
        )
        node.semantic_description = f"{node.formula_kind} formula at {node.cell_ref}"
        ctx.extraction.formulas.append(node)
        formula_key = f"{sheet_key}:formula:{raw_formula.cell_ref}"
        ctx.emit(
            record_id=formula_key,
            object_type="formula",
            title=f"{sheet.sheet_name}!{raw_formula.cell_ref}",
            text=renderers.render_formula(node),
            object_key=formula_key,
            parent_object_key=sheet_key,
            sheet_name=sheet.sheet_name,
            range_ref=raw_formula.cell_ref,
            keyword_text=raw_formula.formula_text,
            extra={"cell_ref": raw_formula.cell_ref, "formula_kind": node.formula_kind},
        )


def _process_pivots(ctx: _Context, raw_sheet: RawSheet, sheet: SheetNode, sheet_key: str) -> None:
    for raw_pivot in raw_sheet.pivots:
        node = PivotNode(
            pivot_id=ids.stable_id(sheet.sheet_id, "pivot", raw_pivot.name),
            sheet_id=sheet.sheet_id,
            pivot_name=raw_pivot.name,
            source_ref=raw_pivot.source_ref,
            row_fields=list(raw_pivot.row_fields),
            column_fields=list(raw_pivot.column_fields),
            value_fields=list(raw_pivot.value_fields),
            filter_fields=list(raw_pivot.filter_fields),
            range_ref=raw_pivot.range_ref,
            extraction_completeness=raw_pivot.extraction_completeness,
        )
        node.summary_text = renderers.render_pivot(node)
        ctx.extraction.pivots.append(node)
        pivot_key = f"{sheet_key}:pivot:{ids.stable_id(raw_pivot.name)}"
        ctx.emit(
            record_id=pivot_key,
            object_type="pivot",
            title=raw_pivot.name,
            text=node.summary_text,
            object_key=pivot_key,
            parent_object_key=sheet_key,
            sheet_name=sheet.sheet_name,
            range_ref=raw_pivot.range_ref,
            keyword_text=" ".join([raw_pivot.name, *raw_pivot.row_fields, *raw_pivot.value_fields]),
            extra={"extraction_completeness": node.extraction_completeness},
        )


def searchable_records_to_records(
    records: list[SearchableRecord],
    *,
    extra_metadata: dict[str, Any] | None = None,
) -> list[Record]:
    """Convert searchable records into backend-neutral ``cloud_dog_vdb`` records.

    Metadata values are flattened to backend-safe scalars (lists become joined
    strings) so they pass through every vector backend's metadata constraints.
    """
    extra_metadata = extra_metadata or {}
    out: list[Record] = []
    for rec in records:
        metadata = {k: _scalar(v) for k, v in rec.metadata.items()}
        metadata.update(
            {
                "title": rec.title,
                "object_type": rec.object_type,
                "source_type": rec.source_type,
                "language": rec.language,
                "locale": rec.locale,
                "keyword_text": rec.keyword_text,
                "source_hash": rec.source_hash,
                "delete_key": rec.delete_key,
                "embedding_input_profile": rec.embedding_input_profile,
                "permissions": ",".join(rec.permissions),
            }
        )
        metadata.update({k: _scalar(v) for k, v in extra_metadata.items()})
        out.append(Record(record_id=rec.record_id, content=rec.text, metadata=metadata))
    return out


# --- small helpers -------------------------------------------------------------


def _cell(cells: list[list[Any]], row: int, col: int) -> Any:
    if 0 <= row < len(cells) and 0 <= col < len(cells[row]):
        return cells[row][col]
    return None


def _scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _safe_range(range_ref: str) -> tuple[int, int, int, int] | None:
    try:
        r0, c0, r1, c1 = coords.parse_range_ref(range_ref)
    except ValueError:
        return None
    return r0, c0, r1, c1


def _overlaps_any(bounds: tuple[int, int, int, int], others: list[tuple[int, int, int, int]]) -> bool:
    r0, c0, r1, c1 = bounds
    for o0, oc0, o1, oc1 in others:
        if r0 <= o1 and o0 <= r1 and c0 <= oc1 and oc0 <= c1:
            return True
    return False


def _normalise_columns(header: list[Any]) -> tuple[list[str], list[str]]:
    normalised: list[str] = []
    originals: list[str] = []
    seen: dict[str, int] = {}
    for idx, raw in enumerate(header):
        original = "" if raw is None else str(raw).strip()
        originals.append(original)
        base = " ".join(original.split()) if original else f"column_{idx + 1}"
        candidate = base
        if candidate in seen:
            seen[candidate] += 1
            candidate = f"{base}_{seen[candidate]}"
        else:
            seen[candidate] = 1
        normalised.append(candidate)
    return normalised, originals


def _context_above(cells: list[list[Any]], r0: int, c0: int, c1: int) -> str:
    if r0 <= 0:
        return ""
    above = [_cell(cells, r0 - 1, c) for c in range(c0, c1 + 1)]
    parts = [str(v).strip() for v in above if v is not None and str(v).strip()]
    return " ".join(parts)
