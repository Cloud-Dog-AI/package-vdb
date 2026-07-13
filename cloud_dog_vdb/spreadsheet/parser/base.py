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

"""Parser abstraction and raw parse model (requirements section 11).

A layered parser strategy is used: each concrete parser turns workbook bytes
into a backend-neutral :class:`RawWorkbook`. The pipeline then normalises the
raw model into canonical nodes. Concrete parsers MUST NOT execute macros or any
active workbook content (section 17.1).

The raw model deliberately holds only plain Python values (str/int/float/bool/
datetime/None) plus native A1 references, so it is independent of any parser
library and of any vector/SQL backend.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig


@dataclass
class RawTable:
    """A formal table/database-range definition exposed by the parser (section 5.4)."""

    name: str
    range_ref: str
    display_name: str = ""
    style_name: str = ""
    header_row_index: int = 0
    totals_row_present: bool = False
    column_labels: list[str] = field(default_factory=list)


@dataclass
class RawNamedRange:
    """A workbook- or sheet-level named range (section 5.2)."""

    name: str
    refers_to: str
    scope: str = "workbook"


@dataclass
class RawFormula:
    """A formula cell captured without evaluation (section 5.8, 17.1)."""

    cell_ref: str
    formula_text: str
    display_value: str = ""


@dataclass
class RawPivot:
    """A pivot artefact where parser support allows (section 5.9)."""

    name: str
    source_ref: str = ""
    row_fields: list[str] = field(default_factory=list)
    column_fields: list[str] = field(default_factory=list)
    value_fields: list[str] = field(default_factory=list)
    filter_fields: list[str] = field(default_factory=list)
    range_ref: str = ""
    extraction_completeness: str = "complete"


@dataclass
class RawSheet:
    """Raw extracted worksheet (section 5.3).

    ``cells`` is a dense, zero-based grid of display/typed values bounded by the
    used range. ``merged_cells`` holds native A1 ranges (``A1:C1``).
    """

    name: str
    index: int
    visibility: str = "visible"
    used_range: str = ""
    cells: list[list[Any]] = field(default_factory=list)
    merged_cells: list[str] = field(default_factory=list)
    hidden_rows: list[int] = field(default_factory=list)
    hidden_columns: list[int] = field(default_factory=list)
    frozen_panes: str = ""
    formal_tables: list[RawTable] = field(default_factory=list)
    formulas: list[RawFormula] = field(default_factory=list)
    pivots: list[RawPivot] = field(default_factory=list)


@dataclass
class RawWorkbook:
    """Raw, library-independent parse of a workbook (section 10 stage 2)."""

    file_name: str
    file_format: str
    properties: dict[str, Any] = field(default_factory=dict)
    sheets: list[RawSheet] = field(default_factory=list)
    named_ranges: list[RawNamedRange] = field(default_factory=list)
    has_macros: bool = False
    parse_status: str = "complete"
    warnings: list[str] = field(default_factory=list)


class WorkbookParser(ABC):
    """Contract for a workbook parser (section 11.2).

    Concrete parsers implement the granular extraction steps (workbook metadata,
    sheets, formal tables, named ranges, formulas, pivots) internally and expose
    the result through :meth:`parse`.
    """

    #: File formats (extensions without dot) handled by this parser.
    supported_formats: tuple[str, ...] = ()

    @classmethod
    def supports(cls, file_format: str) -> bool:
        """Return ``True`` if this parser handles ``file_format``."""
        return file_format.lower().lstrip(".") in cls.supported_formats

    @abstractmethod
    def parse(self, data: bytes, *, file_name: str, config: SpreadsheetConfig) -> RawWorkbook:
        """Parse workbook ``data`` into a :class:`RawWorkbook` without executing macros."""


def detect_format(file_name: str, data: bytes) -> str:
    """Best-effort workbook format detection from name then magic bytes (section 5.1)."""
    lowered = file_name.lower()
    for ext in ("xlsx", "xlsm", "xlsb", "xls", "ods"):
        if lowered.endswith("." + ext):
            return ext
    # Magic-byte fallback: OOXML/ODS are ZIP (PK\x03\x04); legacy xls is OLE2.
    if data[:4] == b"PK\x03\x04":
        if b"mimetypeapplication/vnd.oasis.opendocument.spreadsheet" in data[:4096]:
            return "ods"
        return "xlsx"
    if data[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return "xls"
    return ""


def get_parser_for(file_format: str) -> WorkbookParser:
    """Return a parser instance for ``file_format`` (section 11.1 layered strategy).

    Raises :class:`UnsupportedFormatError` when no parser is available.
    """
    # Imported lazily so optional parser dependencies (openpyxl / odfpy) are only
    # required for the formats actually used.
    fmt = file_format.lower().lstrip(".")
    if fmt in ("xlsx", "xlsm"):
        from cloud_dog_vdb.spreadsheet.parser.openpyxl_parser import OpenpyxlParser

        return OpenpyxlParser()
    if fmt == "ods":
        from cloud_dog_vdb.spreadsheet.parser.ods_parser import OdsParser

        return OdsParser()
    raise UnsupportedFormatError(file_format)


class UnsupportedFormatError(ValueError):
    """Raised when no parser supports a requested workbook format."""

    def __init__(self, file_format: str) -> None:
        super().__init__(f"no spreadsheet parser available for format {file_format!r}")
        self.file_format = file_format
