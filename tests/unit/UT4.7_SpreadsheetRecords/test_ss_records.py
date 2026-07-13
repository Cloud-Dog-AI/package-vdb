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

from cloud_dog_vdb.domain.models import Record
from cloud_dog_vdb.spreadsheet import extract_workbook, searchable_records_to_records, testing


def test_records_are_backend_neutral_with_scalar_metadata():
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", source_uri="file:///s.xlsx")
    records = searchable_records_to_records(ex.searchable_records)
    assert records and all(isinstance(r, Record) for r in records)
    for record in records:
        assert record.record_id and record.content
        for key, value in record.metadata.items():
            assert value is None or isinstance(value, (str, int, float, bool)), (key, type(value))
        assert record.metadata["object_type"] in (
            "workbook", "sheet", "table", "column", "row_batch", "named_range",
        )
        assert record.metadata["delete_key"]
        assert record.metadata["source_type"] == "excel"


def test_extra_metadata_is_merged():
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx")
    records = searchable_records_to_records(ex.searchable_records, extra_metadata={"tenant_id": "acme"})
    assert all(r.metadata["tenant_id"] == "acme" for r in records)


def test_record_ids_match_searchable_record_ids():
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx")
    records = searchable_records_to_records(ex.searchable_records)
    assert [r.record_id for r in records] == [r.record_id for r in ex.searchable_records]
