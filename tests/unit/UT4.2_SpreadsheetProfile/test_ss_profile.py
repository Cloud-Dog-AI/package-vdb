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

from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.detect.profile import infer_data_type, profile_column


def test_infer_homogeneous_types():
    assert infer_data_type([1, 2, 3]) == "integer"
    assert infer_data_type([1.5, 2.0]) == "decimal"
    assert infer_data_type(["a", "b"]) == "text"
    assert infer_data_type([True, False]) == "boolean"
    assert infer_data_type([dt.date(2020, 1, 1), dt.date(2021, 2, 2)]) == "date"


def test_infer_collapses_numeric_and_dates():
    assert infer_data_type([1, 2.5, 3]) == "decimal"
    assert infer_data_type([dt.date(2020, 1, 1), dt.datetime(2021, 2, 2, 10, 0)]) == "datetime"


def test_infer_string_money_and_percent():
    assert infer_data_type(["12%", "5%"]) == "percentage"
    assert infer_data_type(["$1,200.50", "$3.00"]) == "currency"


def test_infer_empty_and_mixed():
    assert infer_data_type([None, "", "  "]) == "empty"
    assert infer_data_type([1, "alpha", dt.date(2020, 1, 1)]) == "mixed"


def test_profile_metrics():
    config = SpreadsheetConfig(sample_value_count=3)
    profile = profile_column(["a", "b", "a", None, "c"], config)
    assert profile.data_type_hint == "text"
    assert profile.non_null_count == 4
    assert profile.distinct_estimate == 3
    assert profile.null_ratio == 0.2
    assert len(profile.sample_values) == 3
