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

"""Region classification (requirements section 12.2).

Classifies a detected region as ``dataset``, ``report_grid``, ``pivot_output``,
``notes`` or ``decorative``. Only ``dataset``, ``report_grid`` and
``pivot_output`` normally proceed to chunking and indexing.
"""

from __future__ import annotations

from typing import Any

from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.coords import parse_range_ref
from cloud_dog_vdb.spreadsheet.detect.profile import infer_data_type
from cloud_dog_vdb.spreadsheet.detect.regions import DetectedRegion
from cloud_dog_vdb.spreadsheet.parser.base import RawSheet

_NUMERIC_TYPES = ("integer", "decimal", "currency", "percentage")
_LONG_TEXT = 60


def _cell(cells: list[list[Any]], row: int, col: int) -> Any:
    if 0 <= row < len(cells) and 0 <= col < len(cells[row]):
        return cells[row][col]
    return None


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def classify_region(sheet: RawSheet, region: DetectedRegion, config: SpreadsheetConfig) -> tuple[str, float]:
    """Return ``(classification, confidence)`` for a detected region (section 12.2)."""
    cells = sheet.cells
    rows = region.row_count
    cols = region.col_count

    if rows < config.min_region_rows or (rows <= 1 and cols <= 2):
        return "decorative", 0.6

    body_start = (
        region.start_row + 1 if region.header_confidence >= config.header_confidence_threshold else region.start_row
    )
    numeric = textual = filled = long_text = 0
    for r in range(body_start, region.end_row + 1):
        for c in range(region.start_col, region.end_col + 1):
            value = _cell(cells, r, c)
            if _blank(value):
                continue
            filled += 1
            if isinstance(value, bool):
                textual += 1
            elif isinstance(value, str):
                textual += 1
                if len(value.strip()) > _LONG_TEXT:
                    long_text += 1
            else:
                numeric += 1
    total_body = max(filled, 1)
    numeric_frac = numeric / total_body
    text_frac = textual / total_body

    # Single-column blocks are lists or free-form notes, not tables.
    if cols == 1:
        if numeric_frac >= 0.7 and rows >= 3:
            return "dataset", 0.4
        if long_text >= 1 or text_frac >= 0.7:
            return "notes", round(0.5 + 0.4 * text_frac, 4)
        return "decorative", 0.5

    # A titled label-column + numeric matrix is a report grid / cross-tab.
    if _has_title_above(sheet, region) and _first_column_is_labels(cells, region) and numeric_frac >= 0.6 and cols >= 3:
        return "report_grid", round(0.5 + 0.4 * numeric_frac, 4)

    # Strong header + a real body => dataset.
    if region.header_confidence >= config.header_confidence_threshold and rows >= 2:
        return "dataset", round(0.5 + 0.5 * region.header_confidence, 4)

    # Predominantly long free-form text with a weak header => notes.
    if long_text >= 2 and numeric_frac < 0.2:
        return "notes", round(0.5 + 0.3 * text_frac, 4)

    # Rectangular and reasonably filled => still a dataset, lower confidence.
    if rows >= 2 and cols >= 2 and filled >= rows:
        return "dataset", round(0.3 + 0.3 * region.header_confidence, 4)

    return "decorative", 0.5


def _first_column_is_labels(cells: list[list[Any]], region: DetectedRegion) -> bool:
    """True if the first column is mostly text labels and the rest mostly numeric."""
    if region.col_count < 2:
        return False
    first_col = [_cell(cells, r, region.start_col) for r in range(region.start_row + 1, region.end_row + 1)]
    label_frac = _text_fraction(first_col)
    rest_types: list[str] = []
    for c in range(region.start_col + 1, region.end_col + 1):
        column = [_cell(cells, r, c) for r in range(region.start_row + 1, region.end_row + 1)]
        rest_types.append(infer_data_type(column))
    numeric_cols = sum(1 for t in rest_types if t in _NUMERIC_TYPES)
    return label_frac >= 0.6 and rest_types != [] and numeric_cols / len(rest_types) >= 0.6


def _has_title_above(sheet: RawSheet, region: DetectedRegion) -> bool:
    """True if a merged title row sits directly above the region (section 12.1)."""
    title_row = region.start_row - 1
    if title_row < 0:
        return False
    width = region.col_count
    for merged in sheet.merged_cells:
        try:
            mr0, mc0, mr1, mc1 = parse_range_ref(merged)
        except ValueError:
            continue
        if mr0 <= title_row <= mr1:
            overlap = min(mc1, region.end_col) - max(mc0, region.start_col) + 1
            if overlap >= max(2, width // 2):
                return True
    return False


def _text_fraction(values: list[Any]) -> float:
    filled = [v for v in values if not _blank(v)]
    if not filled:
        return 0.0
    text = sum(1 for v in filled if isinstance(v, str))
    return text / len(filled)
