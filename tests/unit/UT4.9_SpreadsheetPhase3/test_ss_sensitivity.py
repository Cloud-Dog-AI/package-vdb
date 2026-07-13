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

import io

import openpyxl

from cloud_dog_vdb.spreadsheet import extract_workbook, testing
from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.sensitivity import SensitivityPolicy


def _xlsx(rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    for row in rows:
        ws.append(row)
    buffer = io.BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def test_default_policy_is_inactive():
    policy = SensitivityPolicy.from_config(SpreadsheetConfig())
    assert policy.active() is False
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx")
    assert "[REDACTED]" not in "\n".join(r.text for r in ex.searchable_records)


def test_excluded_sheet_is_skipped():
    config = SpreadsheetConfig(exclude_sheet_patterns=["Costs"])
    ex = extract_workbook(testing.build_multisheet_formal_tables_xlsx(), file_name="m.xlsx", config=config)
    assert {s.sheet_name for s in ex.sheets} == {"Revenue"}
    assert all(r.metadata.get("sheet_name") != "Costs" for r in ex.searchable_records)


def test_excluded_column_withheld_and_redacted():
    config = SpreadsheetConfig(exclude_column_patterns=["Customer"])
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", config=config)
    column_names = [r.metadata["column_name"] for r in ex.searchable_records if r.object_type == "column"]
    assert "Customer" not in column_names
    assert "Amount" in column_names
    row_text = "\n".join(r.text for r in ex.searchable_records if r.object_type == "row_batch")
    assert "[REDACTED]" in row_text
    assert "Acme Ltd" not in row_text


def test_value_redaction_for_emails():
    config = SpreadsheetConfig(redact_sensitive=True)
    ex = extract_workbook(_xlsx([["Name", "Email"], ["Al", "al@example.com"]]), file_name="e.xlsx", config=config)
    blob = "\n".join(r.text for r in ex.searchable_records)
    assert "al@example.com" not in blob
    assert ex.stats.get("redactions", 0) >= 1


def test_custom_sensitivity_pattern():
    config = SpreadsheetConfig(redact_sensitive=True, sensitivity_patterns=[r"SECRET-\d+"])
    ex = extract_workbook(_xlsx([["Key", "Token"], ["k", "SECRET-12345"]]), file_name="t.xlsx", config=config)
    blob = "\n".join(r.text for r in ex.searchable_records)
    assert "SECRET-12345" not in blob
