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

import pytest

from cloud_dog_vdb.spreadsheet import ExtractionQuery, extract_workbook, run_query, testing


@pytest.fixture
def extraction():
    return extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", source_uri="file:///s.xlsx")


def test_query_by_table(extraction):
    results = run_query(extraction, ExtractionQuery(table="tbl_sales"))
    assert len(results) == 1
    result = results[0]
    assert result.columns == ["ID", "Customer", "Amount", "Date"]
    assert len(result.rows) == 3
    assert result.provenance["table_name"] == "tbl_sales"
    assert result.coordinates["range_ref"] == "A1:D4"


def test_query_column_projection(extraction):
    results = run_query(extraction, ExtractionQuery(table="tbl_sales", columns=["Customer", "Amount"]))
    assert results[0].columns == ["Customer", "Amount"]
    assert results[0].rows[0] == ["Acme Ltd", 1200.5]


def test_query_where_predicate(extraction):
    results = run_query(extraction, ExtractionQuery(table="tbl_sales", where={"Customer": "Globex"}))
    assert len(results[0].rows) == 1
    assert results[0].rows[0][1] == "Globex"


def test_query_row_interval(extraction):
    # data rows occupy spreadsheet rows 2-4; restrict to rows 2-3
    results = run_query(extraction, ExtractionQuery(table="tbl_sales", rows={"start": 2, "end": 3}))
    assert len(results[0].rows) == 2


def test_query_missing_columns_warn(extraction):
    results = run_query(extraction, ExtractionQuery(table="tbl_sales", columns=["Customer", "Nope"]))
    assert results[0].columns == ["Customer"]
    assert any("Nope" in w for w in results[0].warnings)


def test_query_by_named_range():
    extraction = extract_workbook(testing.build_formula_named_range_xlsx(), file_name="calc.xlsx")
    results = run_query(extraction, ExtractionQuery(named_range="ValueColumn"))
    assert len(results) == 1
    assert results[0].object_type == "named_range"
    assert results[0].provenance["name"] == "ValueColumn"


def test_query_by_object_type_sheet(extraction):
    results = run_query(extraction, ExtractionQuery(object_type="sheet"))
    assert results and all(r.object_type == "sheet" for r in results)
