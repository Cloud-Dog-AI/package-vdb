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

from cloud_dog_vdb.spreadsheet import build_search_request, detect_query_language
from cloud_dog_vdb.spreadsheet.retrieval import RETRIEVAL_VIEWS


def test_conceptual_view_request():
    req = build_search_request("revenue trends", view="conceptual", top_k=7)
    assert req.query_text == "revenue trends"
    assert req.top_k == 7
    assert req.filters["source_type"] == "excel"
    assert req.query_plan_hints["retrieval_view"] == "conceptual"
    assert req.query_plan_hints["hybrid"] is True
    assert req.query_plan_hints["object_types"] == list(RETRIEVAL_VIEWS["conceptual"])


def test_schema_view_with_table_filter():
    req = build_search_request("column types", view="schema", table="tbl_revenue", sheet="Revenue")
    assert req.filters["table_name"] == "tbl_revenue"
    assert req.filters["sheet_name"] == "Revenue"
    assert req.query_plan_hints["object_types"] == ["column", "table"]


def test_query_language_detection_sets_hint():
    req = build_search_request("収益の推移", view="factual")
    assert req.query_plan_hints["query_language"] == "ja"
    assert req.query_plan_hints["boost_language_match"] is True


def test_detect_query_language():
    assert detect_query_language("región de ventas") == "en"
    assert detect_query_language("销售区域") == "zh"


def test_unknown_view_raises():
    with pytest.raises(ValueError):
        build_search_request("x", view="bogus")
