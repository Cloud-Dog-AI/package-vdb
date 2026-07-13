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


def _row_batches(ex):
    return [r for r in ex.searchable_records if r.object_type == "row_batch"]


def test_row_policy_none_emits_no_row_records():
    ex = extract_workbook(
        testing.build_simple_xlsx(), file_name="s.xlsx", config=SpreadsheetConfig(row_indexing_policy="none")
    )
    assert _row_batches(ex) == []


def test_row_policy_sampled_caps_records():
    config = SpreadsheetConfig(row_indexing_policy="sampled", row_sample_size=10)
    ex = extract_workbook(testing.build_large_xlsx(500), file_name="big.xlsx", config=config)
    batches = _row_batches(ex)
    assert 0 < len(batches) <= 10
    assert all(b.metadata["record_count"] == 1 for b in batches)


def test_row_policy_key_only_keeps_only_key_column():
    config = SpreadsheetConfig(row_indexing_policy="key_only")
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", config=config)
    text = "\n".join(b.text for b in _row_batches(ex))
    assert "ID=" in text
    assert "Customer=" not in text


def test_row_policy_all_is_default():
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx")
    assert len(_row_batches(ex)) == 3
