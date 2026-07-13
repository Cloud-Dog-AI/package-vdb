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

from cloud_dog_vdb.spreadsheet import coords


@pytest.mark.parametrize(
    "index,letter",
    [(0, "A"), (25, "Z"), (26, "AA"), (27, "AB"), (51, "AZ"), (701, "ZZ"), (702, "AAA")],
)
def test_column_letter_roundtrip(index, letter):
    assert coords.column_index_to_letter(index) == letter
    assert coords.column_letter_to_index(letter) == index


def test_cell_and_range_refs():
    assert coords.cell_ref(0, 0) == "A1"
    assert coords.cell_ref(49, 3) == "D50"
    assert coords.range_ref(0, 0, 49, 3) == "A1:D50"


def test_parse_cell_ref_handles_absolute():
    assert coords.parse_cell_ref("$D$50") == (49, 3)
    assert coords.parse_cell_ref("A1") == (0, 0)


def test_parse_range_ref_single_and_normalised():
    assert coords.parse_range_ref("A1") == (0, 0, 0, 0)
    assert coords.parse_range_ref("A1:D50") == (0, 0, 49, 3)
    # reversed corners are normalised to top-left/bottom-right
    assert coords.parse_range_ref("D50:A1") == (0, 0, 49, 3)


def test_invalid_refs_raise():
    with pytest.raises(ValueError):
        coords.parse_cell_ref("1A")
    with pytest.raises(ValueError):
        coords.column_letter_to_index("4")
    with pytest.raises(ValueError):
        coords.column_index_to_letter(-1)
