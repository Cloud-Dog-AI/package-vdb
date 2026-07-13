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

"""Inferred tabular region detection (requirements section 5.5, 12.1).

The detector splits a sheet's used range into rectangular blocks separated by
fully-blank rows and columns, trims each to its bounding box, and scores how
likely the top row is a header. Classification (section 12.2) is applied
separately by :mod:`cloud_dog_vdb.spreadsheet.detect.classify`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.detect.profile import infer_data_type
from cloud_dog_vdb.spreadsheet.parser.base import RawSheet


@dataclass
class DetectedRegion:
    """A rectangular candidate region in a sheet (zero-based, inclusive bounds)."""

    start_row: int
    end_row: int
    start_col: int
    end_col: int
    header_row_index: int
    header_confidence: float
    classification: str = ""
    classification_confidence: float = 0.0

    @property
    def row_count(self) -> int:
        return self.end_row - self.start_row + 1

    @property
    def col_count(self) -> int:
        return self.end_col - self.start_col + 1


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _cell(cells: list[list[Any]], row: int, col: int) -> Any:
    if 0 <= row < len(cells) and 0 <= col < len(cells[row]):
        return cells[row][col]
    return None


def _row_blank(cells: list[list[Any]], row: int, c0: int, c1: int) -> bool:
    return all(_blank(_cell(cells, row, c)) for c in range(c0, c1 + 1))


def _col_blank(cells: list[list[Any]], col: int, r0: int, r1: int) -> bool:
    return all(_blank(_cell(cells, r, col)) for r in range(r0, r1 + 1))


def _grid_bounds(cells: list[list[Any]]) -> tuple[int, int]:
    max_row = len(cells)
    max_col = max((len(row) for row in cells), default=0)
    return max_row, max_col


def _row_bands(cells: list[list[Any]], max_row: int, max_col: int) -> list[tuple[int, int]]:
    bands: list[tuple[int, int]] = []
    start: int | None = None
    for row in range(max_row):
        blank = _row_blank(cells, row, 0, max_col - 1)
        if not blank and start is None:
            start = row
        elif blank and start is not None:
            bands.append((start, row - 1))
            start = None
    if start is not None:
        bands.append((start, max_row - 1))
    return bands


def _col_bands(cells: list[list[Any]], r0: int, r1: int, max_col: int) -> list[tuple[int, int]]:
    bands: list[tuple[int, int]] = []
    start: int | None = None
    for col in range(max_col):
        blank = _col_blank(cells, col, r0, r1)
        if not blank and start is None:
            start = col
        elif blank and start is not None:
            bands.append((start, col - 1))
            start = None
    if start is not None:
        bands.append((start, max_col - 1))
    return bands


def _trim(cells: list[list[Any]], r0: int, r1: int, c0: int, c1: int) -> tuple[int, int, int, int] | None:
    while r0 <= r1 and _row_blank(cells, r0, c0, c1):
        r0 += 1
    while r1 >= r0 and _row_blank(cells, r1, c0, c1):
        r1 -= 1
    while c0 <= c1 and _col_blank(cells, c0, r0, r1):
        c0 += 1
    while c1 >= c0 and _col_blank(cells, c1, r0, r1):
        c1 -= 1
    if r0 > r1 or c0 > c1:
        return None
    return r0, r1, c0, c1


def header_confidence(cells: list[list[Any]], r0: int, r1: int, c0: int, c1: int) -> float:
    """Score in [0, 1] estimating whether row ``r0`` is a header for the block.

    Combines two signals (section 12.1): the fraction of top-row cells that look
    like text labels, and the fraction of columns whose body data type differs
    from a text header.
    """
    header = [_cell(cells, r0, c) for c in range(c0, c1 + 1)]
    width = len(header)
    if width == 0:
        return 0.0
    label_like = sum(1 for v in header if isinstance(v, str) and v.strip() != "")
    frac_label = label_like / width

    if r1 <= r0:
        return round(0.6 * frac_label, 4)

    contrast = 0
    for idx, col in enumerate(range(c0, c1 + 1)):
        head_val = header[idx]
        head_is_text = isinstance(head_val, str) and head_val.strip() != ""
        body = [_cell(cells, r, col) for r in range(r0 + 1, r1 + 1)]
        body_type = infer_data_type(body)
        if head_is_text and body_type not in ("text", "empty", "mixed"):
            contrast += 1
    frac_contrast = contrast / width
    return round(0.5 * frac_label + 0.5 * frac_contrast, 4)


def detect_regions(sheet: RawSheet, config: SpreadsheetConfig) -> list[DetectedRegion]:
    """Detect candidate rectangular regions in a sheet (section 12.1)."""
    cells = sheet.cells
    if not cells:
        return []
    max_row, max_col = _grid_bounds(cells)
    if max_row == 0 or max_col == 0:
        return []

    regions: list[DetectedRegion] = []
    for r0, r1 in _row_bands(cells, max_row, max_col):
        for c0, c1 in _col_bands(cells, r0, r1, max_col):
            trimmed = _trim(cells, r0, r1, c0, c1)
            if trimmed is None:
                continue
            tr0, tr1, tc0, tc1 = trimmed
            # Skip up to a few leading single-cell "title" rows so the real header
            # is used and the title is left directly above as context (section 12.1).
            header_idx = _skip_title_rows(cells, tr0, tr1, tc0, tc1)
            rows = tr1 - header_idx + 1
            cols = tc1 - tc0 + 1
            if rows < config.min_region_rows or cols < config.min_region_cols:
                continue
            confidence = header_confidence(cells, header_idx, tr1, tc0, tc1)
            regions.append(
                DetectedRegion(
                    start_row=header_idx,
                    end_row=tr1,
                    start_col=tc0,
                    end_col=tc1,
                    header_row_index=header_idx,
                    header_confidence=confidence,
                )
            )
    return regions


def _skip_title_rows(cells: list[list[Any]], r0: int, r1: int, c0: int, c1: int) -> int:
    """Return the first row that looks like a header, skipping leading title rows."""
    if c1 <= c0:
        return r0
    header_idx = r0
    limit = min(r0 + 3, r1)
    while header_idx < limit:
        non_blank = sum(1 for c in range(c0, c1 + 1) if not _blank(_cell(cells, header_idx, c)))
        if non_blank <= 1:
            header_idx += 1
        else:
            break
    return header_idx
