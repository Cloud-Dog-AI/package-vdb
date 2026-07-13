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

"""Spreadsheet coordinate helpers.

The canonical model uses a zero-based ``(row, column)`` coordinate space while
preserving native A1-style addresses (``A1``, ``A1:D50``) for provenance, as
required by requirements section 5.16.
"""

from __future__ import annotations

import re

_CELL_RE = re.compile(r"^\$?([A-Za-z]+)\$?([0-9]+)$")


def column_index_to_letter(index: int) -> str:
    """Convert a zero-based column index into a spreadsheet column letter.

    ``0 -> 'A'``, ``25 -> 'Z'``, ``26 -> 'AA'``.
    """
    if index < 0:
        raise ValueError(f"column index must be non-negative, got {index}")
    letters = ""
    remaining = index + 1
    while remaining > 0:
        remaining, modulo = divmod(remaining - 1, 26)
        letters = chr(ord("A") + modulo) + letters
    return letters


def column_letter_to_index(letter: str) -> int:
    """Convert a spreadsheet column letter into a zero-based column index.

    ``'A' -> 0``, ``'Z' -> 25``, ``'AA' -> 26``.
    """
    cleaned = letter.strip().upper()
    if not cleaned or not cleaned.isalpha():
        raise ValueError(f"invalid column letter: {letter!r}")
    index = 0
    for char in cleaned:
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def cell_ref(row: int, column: int) -> str:
    """Render a zero-based ``(row, column)`` as a native A1 address (``A1``)."""
    return f"{column_index_to_letter(column)}{row + 1}"


def range_ref(start_row: int, start_column: int, end_row: int, end_column: int) -> str:
    """Render a zero-based bounding box as a native A1 range (``A1:D50``)."""
    return f"{cell_ref(start_row, start_column)}:{cell_ref(end_row, end_column)}"


def parse_cell_ref(ref: str) -> tuple[int, int]:
    """Parse a native cell address into zero-based ``(row, column)``."""
    match = _CELL_RE.match(ref.strip())
    if match is None:
        raise ValueError(f"invalid cell reference: {ref!r}")
    column = column_letter_to_index(match.group(1))
    row = int(match.group(2)) - 1
    return row, column


def parse_range_ref(ref: str) -> tuple[int, int, int, int]:
    """Parse a native A1 range into zero-based ``(r0, c0, r1, c1)``.

    A single-cell reference (``A1``) yields a one-cell range. The returned
    coordinates are normalised so the start is the top-left corner.
    """
    parts = ref.strip().split(":")
    if len(parts) == 1:
        row, column = parse_cell_ref(parts[0])
        return row, column, row, column
    if len(parts) != 2:
        raise ValueError(f"invalid range reference: {ref!r}")
    r0, c0 = parse_cell_ref(parts[0])
    r1, c1 = parse_cell_ref(parts[1])
    return min(r0, r1), min(c0, c1), max(r0, r1), max(c0, c1)
