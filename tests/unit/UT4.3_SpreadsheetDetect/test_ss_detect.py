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

from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.detect import classify_region, detect_regions
from cloud_dog_vdb.spreadsheet.parser.base import RawSheet

CONFIG = SpreadsheetConfig()


def _sheet(cells, merged=None):
    return RawSheet(name="S", index=0, cells=cells, merged_cells=merged or [])


def test_detect_single_dataset_region():
    sheet = _sheet([["Name", "Age"], ["Al", 30], ["Bo", 25], ["Ci", 40]])
    regions = detect_regions(sheet, CONFIG)
    assert len(regions) == 1
    region = regions[0]
    assert (region.start_row, region.start_col, region.end_row, region.end_col) == (0, 0, 3, 1)
    assert region.header_confidence > 0.5
    assert classify_region(sheet, region, CONFIG)[0] == "dataset"


def test_detect_two_blocks_separated_by_blank_row():
    sheet = _sheet(
        [
            ["H1", "H2"],
            ["a", 1],
            [None, None],
            ["H3", "H4"],
            ["b", 2],
            ["c", 3],
        ]
    )
    regions = detect_regions(sheet, CONFIG)
    assert len(regions) == 2


def test_notes_block_excluded():
    sheet = _sheet(
        [
            ["This is a long free-form note that clearly exceeds sixty characters in length here."],
            ["Another commentary sentence also well beyond the sixty character threshold for notes."],
        ]
    )
    regions = detect_regions(sheet, CONFIG)
    assert len(regions) == 1
    assert classify_region(sheet, regions[0], CONFIG)[0] == "notes"


def test_report_grid_with_title_above():
    sheet = _sheet(
        [
            ["Sales by Region", None, None, None],
            ["Region", "Q1", "Q2", "Q3"],
            ["North", 100, 120, 130],
            ["South", 90, 95, 99],
            ["East", 70, 80, 85],
        ],
        merged=["A1:D1"],
    )
    regions = detect_regions(sheet, CONFIG)
    assert len(regions) == 1
    region = regions[0]
    # the title row is skipped; the real header is row index 1
    assert region.header_row_index == 1
    assert classify_region(sheet, region, CONFIG)[0] == "report_grid"


def test_decorative_tiny_block():
    sheet = _sheet([["x", "y"]])
    regions = detect_regions(sheet, CONFIG)
    # a single row is below the minimum region size
    assert regions == []
