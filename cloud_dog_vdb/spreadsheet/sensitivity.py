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

"""Sensitivity rules and exclusion policies (requirements section 17.3, Phase 3).

A policy can exclude whole sheets or named columns from indexing (glob patterns,
case-insensitive) and redact values that match sensitive-content patterns. When
``redact_sensitive`` is enabled with no custom patterns, a conservative built-in
set (card-like numbers, SSNs, emails) is used.
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field

from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig

REDACTION = "[REDACTED]"

_DEFAULT_PATTERNS = (
    r"\b\d{13,19}\b",  # card-like long numeric runs
    r"\b\d{3}-\d{2}-\d{4}\b",  # US SSN
    r"\b[\w.%+-]+@[\w.-]+\.[A-Za-z]{2,}\b",  # email addresses
)


@dataclass
class SensitivityPolicy:
    """Resolved sensitivity/exclusion policy for an indexing job (section 17.3)."""

    exclude_sheets: list[str] = field(default_factory=list)
    exclude_columns: list[str] = field(default_factory=list)
    redact: bool = False
    patterns: list[re.Pattern[str]] = field(default_factory=list)

    @classmethod
    def from_config(cls, config: SpreadsheetConfig) -> SensitivityPolicy:
        patterns = [re.compile(p) for p in (config.sensitivity_patterns or [])]
        if config.redact_sensitive and not patterns:
            patterns = [re.compile(p) for p in _DEFAULT_PATTERNS]
        return cls(
            exclude_sheets=list(config.exclude_sheet_patterns),
            exclude_columns=list(config.exclude_column_patterns),
            redact=config.redact_sensitive,
            patterns=patterns,
        )

    def active(self) -> bool:
        """Return ``True`` if the policy does anything (saves work when inactive)."""
        return bool(self.exclude_sheets or self.exclude_columns or self.redact)

    def sheet_excluded(self, name: str) -> bool:
        return _match_any(name, self.exclude_sheets)

    def column_excluded(self, name: str) -> bool:
        return _match_any(name, self.exclude_columns)

    def redact_text(self, text: str) -> tuple[str, int]:
        """Redact sensitive substrings; return ``(redacted_text, redaction_count)``."""
        if not self.redact or not text:
            return text, 0
        count = 0
        for pattern in self.patterns:
            text, replaced = pattern.subn(REDACTION, text)
            count += replaced
        return text, count


def _match_any(name: str, patterns: list[str]) -> bool:
    lowered = (name or "").lower()
    return any(fnmatch.fnmatch(lowered, pattern.lower()) for pattern in patterns)
