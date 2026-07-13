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

"""Spreadsheet indexing configuration and controls (requirements section 5.15).

This is a plain value object. The hosting service resolves the actual values
through ``cloud_dog_config`` and passes a populated :class:`SpreadsheetConfig`
in — this package never reads the environment directly (RULES section 1.4).
"""

from __future__ import annotations

from dataclasses import dataclass, field

#: Extraction modes (section 5.15 table/range extraction mode).
EXTRACTION_MODES = ("strict", "balanced", "aggressive")

#: Refresh modes (section 5.15 / 5.18).
REFRESH_MODES = ("full", "incremental", "manifest_only", "delete_stale")

#: Pivot extraction modes (section 5.9 / 5.15).
PIVOT_MODES = ("off", "summary", "full")

#: Row-level selective indexing policies (section 5.7, Phase 3).
ROW_INDEXING_POLICIES = ("all", "sampled", "key_only", "none")


@dataclass
class SpreadsheetConfig:
    """Resolved controls for a spreadsheet indexing job (section 5.15)."""

    # Size and volume controls (section 5.15, 13.3).
    max_workbook_bytes: int = 256 * 1024 * 1024
    max_rows_per_table: int = 100_000
    max_sheets_to_profile: int = 200
    max_cells_per_sheet_scan: int = 2_000_000

    # Row indexing policy (section 13.2).
    row_batch_size: int = 50
    row_level_max_rows: int = 200
    index_row_batches: bool = True

    # Object extraction toggles (section 5.8, 5.9, 5.15).
    extract_formulas: bool = True
    pivot_extraction_mode: str = "summary"
    include_hidden_sheets: bool = True

    # Inferred-table heuristics thresholds (section 5.5, 12.1).
    min_region_rows: int = 2
    min_region_cols: int = 1
    header_confidence_threshold: float = 0.5

    # Table/range extraction mode (section 5.15).
    extraction_mode: str = "balanced"

    # Refresh policy (section 5.15, 5.18).
    refresh_mode: str = "incremental"

    # Format outputs (section 5.16).
    emit_parquet: bool = False
    emit_csv: bool = False

    # Backend/embedding selection hints (resolved + applied by the caller).
    embedding_model: str = ""
    embedding_input_profile: str = "default"
    backend: str = ""
    sql_metadata_backend: str = "sqlite"

    # Parser strategy (section 5.15, 11).
    ods_parser_strategy: str = "odf"

    # Internationalisation (section 5.15, 5.19).
    detect_language: bool = True
    locale: str = ""
    decimal_separator: str = "."
    thousands_separator: str = ","
    date_formats: list[str] = field(default_factory=list)
    generate_translated_aliases: bool = False

    # Sampling for representative values (section 5.6, 13.3).
    sample_value_count: int = 5

    # Row-level selective indexing policy (section 5.7, Phase 3).
    row_indexing_policy: str = "all"
    row_sample_size: int = 50

    # Sensitivity rules and exclusion policies (section 17.3, Phase 3).
    exclude_sheet_patterns: list[str] = field(default_factory=list)
    exclude_column_patterns: list[str] = field(default_factory=list)
    redact_sensitive: bool = False
    sensitivity_patterns: list[str] = field(default_factory=list)

    def validate(self) -> None:
        """Raise ``ValueError`` if any enumerated control is invalid."""
        if self.extraction_mode not in EXTRACTION_MODES:
            raise ValueError(f"invalid extraction_mode: {self.extraction_mode!r}")
        if self.refresh_mode not in REFRESH_MODES:
            raise ValueError(f"invalid refresh_mode: {self.refresh_mode!r}")
        if self.pivot_extraction_mode not in PIVOT_MODES:
            raise ValueError(f"invalid pivot_extraction_mode: {self.pivot_extraction_mode!r}")
        if self.row_batch_size < 1:
            raise ValueError("row_batch_size must be >= 1")
        if not 0.0 <= self.header_confidence_threshold <= 1.0:
            raise ValueError("header_confidence_threshold must be in [0, 1]")
        if self.row_indexing_policy not in ROW_INDEXING_POLICIES:
            raise ValueError(f"invalid row_indexing_policy: {self.row_indexing_policy!r}")
