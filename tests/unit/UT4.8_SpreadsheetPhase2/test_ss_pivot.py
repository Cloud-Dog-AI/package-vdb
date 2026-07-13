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

"""Pivot extraction: parser mapping (stubbed openpyxl objects) + pipeline emission."""

from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.parser.base import RawPivot, RawSheet, RawWorkbook
from cloud_dog_vdb.spreadsheet.parser.openpyxl_parser import _pivot_from_definition
from cloud_dog_vdb.spreadsheet.pipeline import _build_extraction


class _Loc:
    ref = "A3:D12"


class _Field:
    def __init__(self, name):
        self.name = name


class _Cache:
    cacheFields = [_Field("Region"), _Field("Sales"), _Field("Quarter")]
    cacheSource = None


class _RC:
    def __init__(self, x):
        self.x = x


class _Data:
    def __init__(self, fld, name=None):
        self.fld = fld
        self.name = name


class _Definition:
    name = "PivotTable1"
    location = _Loc()
    cache = _Cache()
    rowFields = [_RC(0)]
    colFields = [_RC(2)]
    pageFields = []
    dataFields = [_Data(1, name="Sum of Sales")]


def test_pivot_from_openpyxl_definition_maps_fields():
    pivot = _pivot_from_definition(_Definition())
    assert pivot.name == "PivotTable1"
    assert pivot.range_ref == "A3:D12"
    assert pivot.row_fields == ["Region"]
    assert pivot.column_fields == ["Quarter"]
    assert pivot.value_fields == ["Sum of Sales"]
    assert pivot.extraction_completeness == "complete"


def test_pipeline_emits_pivot_record():
    raw = RawWorkbook(
        file_name="p.xlsx",
        file_format="xlsx",
        sheets=[
            RawSheet(
                name="Data",
                index=0,
                cells=[["Region", "Sales"], ["North", 10], ["South", 20]],
                pivots=[
                    RawPivot(
                        name="PivotTable1",
                        row_fields=["Region"],
                        value_fields=["Sum of Sales"],
                        range_ref="A3:B6",
                    )
                ],
            )
        ],
    )
    extraction = _build_extraction(raw, "wbid", "upload://p.xlsx", "hash", 100, SpreadsheetConfig(), [])
    pivots = [r for r in extraction.searchable_records if r.object_type == "pivot"]
    assert len(pivots) == 1
    assert pivots[0].title == "PivotTable1"
    assert "Region" in pivots[0].keyword_text


def test_pivot_extraction_off_disables_records():
    raw = RawWorkbook(
        file_name="p.xlsx",
        file_format="xlsx",
        sheets=[
            RawSheet(
                name="Data",
                index=0,
                cells=[["Region", "Sales"], ["North", 10]],
                pivots=[RawPivot(name="P", row_fields=["Region"], value_fields=["Sales"])],
            )
        ],
    )
    extraction = _build_extraction(
        raw, "wbid", "upload://p.xlsx", "hash", 100, SpreadsheetConfig(pivot_extraction_mode="off"), []
    )
    assert not [r for r in extraction.searchable_records if r.object_type == "pivot"]
