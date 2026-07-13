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
from cloud_dog_vdb.spreadsheet.parser.ods_parser import OdsParser

CONFIG = SpreadsheetConfig()


def test_parse_ods_grid_and_types():
    raw = OdsParser().parse(testing.build_simple_ods(), file_name="b.ods", config=CONFIG)
    assert raw.file_format == "ods"
    assert [s.name for s in raw.sheets] == ["Budget", "Meta"]
    budget = raw.sheets[0]
    # A1-origin dense grid with the header in row 0
    assert budget.cells[0] == ["Item", "Amount", "When"]
    # float value typed as float, date typed as date/datetime
    assert isinstance(budget.cells[1][1], float)
    assert isinstance(budget.cells[1][2], (dt.date, dt.datetime))


def test_parse_ods_named_range():
    raw = OdsParser().parse(testing.build_simple_ods(), file_name="b.ods", config=CONFIG)
    assert any(nr.name == "AmountCol" for nr in raw.named_ranges)
