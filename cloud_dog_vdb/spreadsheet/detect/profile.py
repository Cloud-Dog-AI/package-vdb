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

"""Column type inference and light profiling (requirements section 5.6)."""

from __future__ import annotations

import datetime as _dt
import re
from dataclasses import dataclass, field
from typing import Any

from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig

#: Data type hints (section 5.6).
DATA_TYPE_HINTS = (
    "text",
    "integer",
    "decimal",
    "currency",
    "percentage",
    "date",
    "datetime",
    "boolean",
    "mixed",
    "empty",
)

_PERCENT_RE = re.compile(r"^-?\d+(?:[.,]\d+)?\s*%$")
_CURRENCY_RE = re.compile(r"^[\$€£¥₽₩]\s*-?[\d.,]+$|^-?[\d.,]+\s*[\$€£¥₽₩]$")
_INT_RE = re.compile(r"^-?\d{1,3}(?:[,\s]\d{3})*$|^-?\d+$")
_DECIMAL_RE = re.compile(r"^-?\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?$|^-?\d*\.\d+$")


@dataclass
class ColumnProfile:
    """Light profile of a single column's values (section 5.6)."""

    data_type_hint: str = "empty"
    sample_values: list[str] = field(default_factory=list)
    distinct_estimate: int = 0
    null_ratio: float = 0.0
    non_null_count: int = 0
    numeric_min: float | None = None
    numeric_max: float | None = None
    numeric_mean: float | None = None


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _classify_value(value: Any) -> str:
    """Return the data type hint for a single non-blank value."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "decimal"
    if isinstance(value, _dt.datetime):
        # A date-only datetime (midnight) is reported as a date.
        if value.hour == 0 and value.minute == 0 and value.second == 0:
            return "date"
        return "datetime"
    if isinstance(value, _dt.date):
        return "date"
    if isinstance(value, _dt.time):
        return "datetime"
    text = str(value).strip()
    lowered = text.lower()
    if lowered in ("true", "false", "yes", "no"):
        return "boolean"
    if _PERCENT_RE.match(text):
        return "percentage"
    if _CURRENCY_RE.match(text):
        return "currency"
    if _INT_RE.match(text):
        return "integer"
    if _DECIMAL_RE.match(text):
        return "decimal"
    return "text"


def infer_data_type(values: list[Any]) -> str:
    """Infer a column data type hint from its values (section 5.6).

    Returns ``empty`` when there are no values, a single hint when the values
    are homogeneous (integer/decimal mixes resolve to ``decimal``), otherwise
    ``mixed``.
    """
    seen: set[str] = set()
    for value in values:
        if _is_blank(value):
            continue
        seen.add(_classify_value(value))
    if not seen:
        return "empty"
    if len(seen) == 1:
        return next(iter(seen))
    # Numeric families collapse rather than degrading to "mixed".
    if seen <= {"integer", "decimal"}:
        return "decimal"
    if seen <= {"integer", "decimal", "currency"}:
        return "currency"
    if seen <= {"date", "datetime"}:
        return "datetime"
    return "mixed"


def profile_column(values: list[Any], config: SpreadsheetConfig) -> ColumnProfile:
    """Profile a column: type hint, sample values, distinct estimate, null ratio."""
    total = len(values)
    non_null = [v for v in values if not _is_blank(v)]
    distinct: set[str] = set()
    samples: list[str] = []
    for value in non_null:
        rendered = _render_sample(value)
        if rendered not in distinct and len(samples) < config.sample_value_count:
            samples.append(rendered)
        distinct.add(rendered)
    null_ratio = 0.0 if total == 0 else round((total - len(non_null)) / total, 4)
    numbers = [float(v) for v in non_null if isinstance(v, (int, float)) and not isinstance(v, bool)]
    numeric_min = min(numbers) if numbers else None
    numeric_max = max(numbers) if numbers else None
    numeric_mean = round(sum(numbers) / len(numbers), 6) if numbers else None
    return ColumnProfile(
        data_type_hint=infer_data_type(values),
        sample_values=samples,
        distinct_estimate=len(distinct),
        null_ratio=null_ratio,
        non_null_count=len(non_null),
        numeric_min=numeric_min,
        numeric_max=numeric_max,
        numeric_mean=numeric_mean,
    )


def _render_sample(value: Any) -> str:
    if isinstance(value, _dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, _dt.date):
        return value.isoformat()
    return str(value).strip()
