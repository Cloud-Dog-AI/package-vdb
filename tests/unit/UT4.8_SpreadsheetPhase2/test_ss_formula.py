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

from cloud_dog_vdb.spreadsheet import extract_workbook, testing
from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.pipeline import _classify_formula


def test_formula_records_emitted():
    ex = extract_workbook(testing.build_formula_named_range_xlsx(), file_name="calc.xlsx")
    formulas = [r for r in ex.searchable_records if r.object_type == "formula"]
    assert formulas
    rec = formulas[0]
    assert rec.metadata["formula_kind"] == "aggregate"
    assert "=SUM" in rec.text


def test_formula_extraction_can_be_disabled():
    config = SpreadsheetConfig(extract_formulas=False)
    ex = extract_workbook(testing.build_formula_named_range_xlsx(), file_name="calc.xlsx", config=config)
    assert not [r for r in ex.searchable_records if r.object_type == "formula"]


def test_classify_formula_families():
    assert _classify_formula("=SUM(A1:A3)") == "aggregate"
    assert _classify_formula("=VLOOKUP(A1,B:C,2,0)") == "lookup"
    assert _classify_formula("=IF(A1>0,1,0)") == "logical"
    assert _classify_formula("=LEFT(A1,3)") == "text"
    assert _classify_formula("=YEAR(A1)") == "date"
    assert _classify_formula("=A1+B1") == "arithmetic"
    assert _classify_formula("=A1") == "reference"
