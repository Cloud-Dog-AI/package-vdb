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

import json

import pytest

from cloud_dog_vdb.spreadsheet import (
    extract_workbook,
    records_to_jsonl,
    table_to_csv,
    table_to_parquet,
    testing,
    workbook_to_json,
    workbook_to_json_str,
)


@pytest.fixture
def extraction():
    return extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", source_uri="file:///s.xlsx")


def test_workbook_to_json_is_serialisable(extraction):
    payload = workbook_to_json(extraction)
    assert payload["workbook"]["file_name"] == "s.xlsx"
    assert payload["tables"][0]["table_name"] == "tbl_sales"
    # datetimes inside data_rows must be JSON-safe (no exception)
    text = workbook_to_json_str(extraction)
    assert json.loads(text)["tables"][0]["row_count"] == 3


def test_records_to_jsonl_one_line_per_record(extraction):
    jsonl = records_to_jsonl(extraction)
    lines = jsonl.splitlines()
    assert len(lines) == len(extraction.searchable_records)
    first = json.loads(lines[0])
    assert "record_id" in first and "object_type" in first


def test_table_to_csv_header_and_rows(extraction):
    csv_text = table_to_csv(extraction.tables[0])
    lines = csv_text.strip().splitlines()
    assert lines[0] == "ID,Customer,Amount,Date"
    assert len(lines) == 4  # header + 3 data rows


def test_table_to_parquet_roundtrip(extraction):
    pytest.importorskip("pyarrow")
    import io

    import pyarrow.parquet as pq

    data = table_to_parquet(extraction.tables[0])
    table = pq.read_table(io.BytesIO(data))
    assert table.num_rows == 3
    assert set(table.column_names) == {"ID", "Customer", "Amount", "Date"}
