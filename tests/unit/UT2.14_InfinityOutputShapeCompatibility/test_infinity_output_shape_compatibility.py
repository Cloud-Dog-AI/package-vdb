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

from __future__ import annotations

from cloud_dog_vdb.adapters.infinity import InfinityAdapter


def test_ut2_14_rows_from_output_supports_object_rows() -> None:
    output = [
        {"record_id": "r1", "content": "alpha", "metadata_json": "{}"},
        {"record_id": "r2", "content": "beta", "metadata_json": "{}"},
    ]
    rows = InfinityAdapter._rows_from_output(output)
    assert rows == output


def test_ut2_14_rows_from_output_supports_nested_object_rows() -> None:
    output = [
        [{"record_id": "r1"}, {"content": "alpha"}, {"metadata_json": "{}"}],
        [{"record_id": "r2"}, {"content": "beta"}, {"metadata_json": "{}"}],
    ]
    rows = InfinityAdapter._rows_from_output(output)
    assert rows == [
        {"record_id": "r1", "content": "alpha", "metadata_json": "{}"},
        {"record_id": "r2", "content": "beta", "metadata_json": "{}"},
    ]
