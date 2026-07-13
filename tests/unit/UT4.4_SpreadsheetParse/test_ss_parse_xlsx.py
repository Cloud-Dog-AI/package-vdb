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

import datetime as dt

from cloud_dog_vdb.spreadsheet import testing
from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.parser.openpyxl_parser import OpenpyxlParser

CONFIG = SpreadsheetConfig()


def _parse(data, name="wb.xlsx"):
    return OpenpyxlParser().parse(data, file_name=name, config=CONFIG)


def test_parse_simple_formal_table():
    raw = _parse(testing.build_simple_xlsx())
    assert len(raw.sheets) == 1
    sheet = raw.sheets[0]
    # A1-origin dense grid: header in row 0
    assert sheet.cells[0] == ["ID", "Customer", "Amount", "Date"]
    assert len(sheet.formal_tables) == 1
    table = sheet.formal_tables[0]
    assert table.range_ref == "A1:D4"
    assert table.column_labels == ["ID", "Customer", "Amount", "Date"]
    # dates preserved as datetime, not stringified
    assert isinstance(sheet.cells[1][3], (dt.datetime, dt.date))


def test_parse_hidden_sheet_visibility():
    raw = _parse(testing.build_hidden_sheet_xlsx())
    states = {s.name: s.visibility for s in raw.sheets}
    assert states["Public"] == "visible"
    assert states["Secret"] == "hidden"


def test_parse_named_range_and_formula():
    raw = _parse(testing.build_formula_named_range_xlsx())
    assert any(nr.name == "ValueColumn" for nr in raw.named_ranges)
    formulas = [f for s in raw.sheets for f in s.formulas]
    assert any(f.formula_text.startswith("=") and "SUM" in f.formula_text for f in formulas)


def test_macro_detection_does_not_execute():
    # plain xlsx => no macros
    raw = _parse(testing.build_simple_xlsx())
    assert raw.has_macros is False


def test_scan_cap_marks_partial():
    config = SpreadsheetConfig(max_cells_per_sheet_scan=6)
    raw = OpenpyxlParser().parse(testing.build_large_xlsx(50), file_name="big.xlsx", config=config)
    assert raw.parse_status == "partial"
    assert raw.warnings
