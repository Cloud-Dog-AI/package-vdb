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

import math

import pytest

from cloud_dog_vdb.spreadsheet import extract_workbook, testing
from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.parser.base import UnsupportedFormatError
from cloud_dog_vdb.spreadsheet.pipeline import WorkbookTooLargeError, _normalise_columns


def test_simple_object_counts_and_types():
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", source_uri="file:///s.xlsx")
    counts = ex.object_count_by_type()
    assert counts == {"workbook": 1, "sheet": 1, "table": 1, "column": 4, "row_batch": 3}
    table = ex.tables[0]
    assert table.table_kind == "formal"
    assert table.normalised_column_names == ["ID", "Customer", "Amount", "Date"]
    by_name = {c.column_name: c.data_type_hint for c in table.columns}
    assert by_name["ID"] == "integer"
    assert by_name["Amount"] in ("decimal", "currency")
    assert by_name["Date"] in ("date", "datetime")


def test_stable_ids_hashes_and_text_are_deterministic():
    # section 19.4: stable IDs and hashes; consistent chunk text construction
    data = testing.build_multisheet_formal_tables_xlsx()
    a = extract_workbook(data, file_name="m.xlsx", source_uri="file:///m.xlsx")
    b = extract_workbook(data, file_name="m.xlsx", source_uri="file:///m.xlsx")
    assert [r.record_id for r in a.searchable_records] == [r.record_id for r in b.searchable_records]
    assert [r.source_hash for r in a.searchable_records] == [r.source_hash for r in b.searchable_records]
    assert [r.text for r in a.searchable_records] == [r.text for r in b.searchable_records]


def test_provenance_fields_present():
    # section 5.11 provenance
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", source_uri="file:///s.xlsx")
    for rec in ex.searchable_records:
        assert rec.metadata["workbook_id"]
        assert rec.metadata["source_uri"] == "file:///s.xlsx"
        assert rec.metadata["object_type"] == rec.object_type
        assert rec.delete_key == rec.metadata["object_key"]
    table_rec = next(r for r in ex.searchable_records if r.object_type == "table")
    assert table_rec.metadata["sheet_name"] == "Sales"
    assert table_rec.metadata["table_name"] == "tbl_sales"
    assert table_rec.metadata["range_ref"] == "A1:D4"


def test_inferred_tables_detected_without_formal_tables():
    ex = extract_workbook(testing.build_inferred_only_xlsx(), file_name="i.xlsx")
    kinds = [t.table_kind for t in ex.tables]
    # the dataset block becomes an inferred table; the note block is excluded
    assert "inferred" in kinds
    assert all(k != "formal" for k in kinds)
    assert len(ex.tables) == 1


def test_report_grid_classification():
    ex = extract_workbook(testing.build_report_grid_xlsx(), file_name="g.xlsx")
    assert any(t.table_kind == "report_grid" for t in ex.tables)
    grid = next(t for t in ex.tables if t.table_kind == "report_grid")
    assert "Sales by Region" in grid.context_text


def test_hidden_sheet_inclusion_toggle():
    data = testing.build_hidden_sheet_xlsx()
    incl = extract_workbook(data, file_name="h.xlsx", config=SpreadsheetConfig(include_hidden_sheets=True))
    excl = extract_workbook(data, file_name="h.xlsx", config=SpreadsheetConfig(include_hidden_sheets=False))
    assert {s.sheet_name for s in incl.sheets} == {"Public", "Secret"}
    assert {s.sheet_name for s in excl.sheets} == {"Public"}
    assert all(r.metadata.get("sheet_name") != "Secret" for r in excl.searchable_records)


def test_large_table_row_batching():
    config = SpreadsheetConfig(row_level_max_rows=200, row_batch_size=50)
    ex = extract_workbook(testing.build_large_xlsx(500), file_name="big.xlsx", config=config)
    table = ex.tables[0]
    assert table.row_count == 500
    assert len(ex.row_batches) == math.ceil(500 / 50)
    assert ex.row_batches[0].record_count == 50


def test_small_table_row_level_indexing():
    config = SpreadsheetConfig(row_level_max_rows=200)
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", config=config)
    # 3 data rows <= 200 => one record per row
    assert len(ex.row_batches) == 3
    assert all(b.record_count == 1 for b in ex.row_batches)


def test_multilingual_text_preserved():
    # section 5.19: original text preserved, including CJK / RTL / accents
    ex = extract_workbook(testing.build_multilingual_xlsx(), file_name="ml.xlsx")
    blob = "\n".join(r.text for r in ex.searchable_records)
    assert "商品A" in blob
    assert "café" in blob
    assert "מוצר" in blob


def test_malformed_workbook_degrades_gracefully():
    # section 19.4: partial failure handling — no exception, marked failed
    ex = extract_workbook(testing.build_malformed_bytes(), file_name="bad.xlsx")
    assert ex.workbook.parse_status == "failed"
    assert ex.searchable_records == []
    assert ex.workbook.warnings


def test_unsupported_format_raises():
    with pytest.raises(UnsupportedFormatError):
        extract_workbook(b"\x00\x01\x02", file_name="thing.bin")


def test_workbook_too_large_raises():
    config = SpreadsheetConfig(max_workbook_bytes=10)
    with pytest.raises(WorkbookTooLargeError):
        extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", config=config)


def test_duplicate_and_blank_headers_normalised():
    normalised, originals = _normalise_columns(["ID", "Name", "Name", None, "ID"])
    assert normalised == ["ID", "Name", "Name_2", "column_4", "ID_2"]
    assert originals == ["ID", "Name", "Name", "", "ID"]


def test_ods_pipeline_end_to_end():
    ex = extract_workbook(testing.build_simple_ods(), file_name="b.ods")
    assert {s.sheet_name for s in ex.sheets} == {"Budget", "Meta"}
    budget_tables = [t for t in ex.tables if t.sheet_id == ex.sheets[0].sheet_id]
    assert budget_tables, "expected an inferred table on the Budget sheet"
    types = {c.column_name: c.data_type_hint for t in budget_tables for c in t.columns}
    assert any(v in ("decimal", "currency") for v in types.values())
    assert any(v in ("date", "datetime") for v in types.values())
    assert any(r.object_type == "named_range" and r.title == "AmountCol" for r in ex.searchable_records)
