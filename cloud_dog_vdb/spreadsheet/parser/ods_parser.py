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

"""OpenDocument Spreadsheet (``.ods``) parser built on odfpy (section 11.1).

This concrete :class:`WorkbookParser` turns ``.ods`` bytes into a backend-neutral
:class:`RawWorkbook`. It loads the document statically and never executes macros
or any other active content (section 17.1): odfpy only reads the XML payload, and
formulas are captured verbatim without evaluation.

The cells grid is materialised A1-origin (``cells[r][c]`` is the value at
spreadsheet row ``r+1`` / column ``c+1``), dense, and bounded by the used range.
ODS stores horizontal/vertical runs as ``table:number-columns-repeated`` and
``table:number-rows-repeated``; these are expanded, except for the giant trailing
empty runs ODS emits to fill the sheet to its nominal width/height, which are
stripped so the grid stays bounded.
"""

from __future__ import annotations

import io
from datetime import datetime
from typing import Any

import odf.opendocument
import odf.table
import odf.teletype
import odf.text

from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.coords import cell_ref, range_ref
from cloud_dog_vdb.spreadsheet.parser.base import (
    RawFormula,
    RawNamedRange,
    RawSheet,
    RawTable,
    RawWorkbook,
    WorkbookParser,
)

#: ODS uses bounded "max repeat" sentinels to fill sheets to their nominal extent.
#: Repeat counts at or above this are treated as trailing padding and not expanded
#: blindly; only the non-empty prefix of such runs is kept.
_MAX_SANE_REPEAT = 4096


class OdsParser(WorkbookParser):
    """Parse OpenDocument Spreadsheet workbooks via odfpy (section 11)."""

    supported_formats = ("ods",)

    def parse(self, data: bytes, *, file_name: str, config: SpreadsheetConfig) -> RawWorkbook:
        """Parse ``.ods`` ``data`` into a :class:`RawWorkbook` without executing macros."""
        doc = odf.opendocument.load(io.BytesIO(data))

        warnings: list[str] = []
        parse_status = "complete"

        style_display = self._collect_table_style_display(doc)

        sheets: list[RawSheet] = []
        for index, tbl in enumerate(doc.spreadsheet.getElementsByType(odf.table.Table)):
            name = tbl.getAttribute("name") or f"Sheet{index + 1}"
            try:
                sheet, sheet_partial, sheet_warnings = self._parse_sheet(
                    tbl, index=index, name=name, config=config, style_display=style_display
                )
                sheets.append(sheet)
                warnings.extend(sheet_warnings)
                if sheet_partial:
                    parse_status = "partial"
            except Exception as exc:  # noqa: BLE001 - graceful per-sheet degradation (section 14)
                parse_status = "partial"
                warnings.append(f"sheet {name!r} (index {index}) failed to parse: {exc}")

        named_ranges = self._parse_named_ranges(doc, warnings)
        properties = self._parse_properties(doc)

        return RawWorkbook(
            file_name=file_name,
            file_format="ods",
            properties=properties,
            sheets=sheets,
            named_ranges=named_ranges,
            has_macros=False,
            parse_status=parse_status,
            warnings=warnings,
        )

    # -- sheets ---------------------------------------------------------------

    def _parse_sheet(
        self,
        tbl: Any,
        *,
        index: int,
        name: str,
        config: SpreadsheetConfig,
        style_display: dict[str, bool],
    ) -> tuple[RawSheet, bool, list[str]]:
        """Extract one worksheet into a :class:`RawSheet`."""
        sheet_warnings: list[str] = []
        partial = False

        cells: list[list[Any]] = []
        formulas: list[RawFormula] = []
        merged: list[str] = []
        max_width = 0
        cell_budget = max(config.max_cells_per_sheet_scan, 0)
        cell_count = 0
        capped = False

        for row in tbl.getElementsByType(odf.table.TableRow):
            if capped:
                break
            row_repeat = self._int_attr(row, "numberrowsrepeated", default=1)
            # Build the dense list of values for a single physical row.
            row_values, row_meta = self._expand_row(row, current_row=len(cells))

            # Trim trailing empty cells; the used range is bounded by content.
            row_values = self._strip_trailing_none(row_values)
            row_meta = row_meta[: len(row_values)]

            # Record formulas / merges using the actual (row, col) of this row.
            base_row = len(cells)
            self._record_row_objects(row_meta, base_row=base_row, formulas=formulas, merged=merged, config=config)

            # Decide how many times this row is materialised. Huge repeats are the
            # trailing-padding sentinel: an empty repeated row contributes nothing,
            # so we collapse it to a single (empty) row that later trailing-row
            # stripping removes entirely.
            repeats = row_repeat
            if not row_values:
                repeats = 1
            elif row_repeat >= _MAX_SANE_REPEAT:
                repeats = 1
                sheet_warnings.append(
                    f"sheet {name!r}: collapsed oversized row repeat ({row_repeat}) at row {base_row + 1}"
                )

            for _ in range(repeats):
                # Budget check is on materialised cells.
                projected = cell_count + max(len(row_values), 0)
                if cell_budget and projected > cell_budget:
                    capped = True
                    partial = True
                    sheet_warnings.append(f"sheet {name!r}: cell scan budget {cell_budget} exceeded; output truncated")
                    break
                cells.append(list(row_values))
                cell_count = projected
                if len(row_values) > max_width:
                    max_width = len(row_values)

        # Strip trailing fully-empty rows.
        while cells and all(value is None for value in cells[-1]):
            cells.pop()

        # Pad every row to the maximum non-empty width.
        for grid_row in cells:
            if len(grid_row) < max_width:
                grid_row.extend([None] * (max_width - len(grid_row)))

        used_range = ""
        if cells and max_width:
            used_range = range_ref(0, 0, len(cells) - 1, max_width - 1)

        visibility = "visible"
        style_name = tbl.getAttribute("stylename")
        if style_name and style_display.get(style_name) is False:
            visibility = "hidden"

        formal_tables = self._parse_formal_tables(tbl, sheet_warnings)

        sheet = RawSheet(
            name=name,
            index=index,
            visibility=visibility,
            used_range=used_range,
            cells=cells,
            merged_cells=merged,
            formal_tables=formal_tables,
            formulas=formulas if config.extract_formulas else [],
        )
        return sheet, partial, sheet_warnings

    def _expand_row(self, row: Any, *, current_row: int) -> tuple[list[Any], list[dict[str, Any]]]:
        """Expand one ``table-row`` into dense value + per-cell metadata lists.

        Horizontal ``number-columns-repeated`` runs are expanded, except oversized
        trailing runs (>= :data:`_MAX_SANE_REPEAT`) carrying an empty value, which
        are dropped so the giant trailing padding ODS emits is not materialised.
        Metadata (formula / spans) is only attached to the first cell of a run.
        """
        values: list[Any] = []
        meta: list[dict[str, Any]] = []

        for cell in row.childNodes:
            local = self._local_name(cell)
            if local not in ("table-cell", "covered-table-cell"):
                continue

            covered = local == "covered-table-cell"
            value = None if covered else self._cell_value(cell)
            repeat = self._int_attr(cell, "numbercolumnsrepeated", default=1)

            cell_meta: dict[str, Any] = {}
            if not covered:
                formula = cell.getAttribute("formula")
                if formula:
                    cell_meta["formula"] = formula
                    cell_meta["display"] = self._cell_display_str(cell)
                cols_spanned = self._int_attr(cell, "numbercolumnsspanned", default=1)
                rows_spanned = self._int_attr(cell, "numberrowsspanned", default=1)
                if cols_spanned > 1 or rows_spanned > 1:
                    cell_meta["span"] = (rows_spanned, cols_spanned)

            # Oversized empty horizontal runs are the trailing padding sentinel.
            if value is None and not cell_meta and repeat >= _MAX_SANE_REPEAT:
                continue

            for offset in range(repeat):
                values.append(value)
                # Metadata only meaningful for the run's first physical cell.
                meta.append(cell_meta if offset == 0 else {})

        return values, meta

    def _record_row_objects(
        self,
        row_meta: list[dict[str, Any]],
        *,
        base_row: int,
        formulas: list[RawFormula],
        merged: list[str],
        config: SpreadsheetConfig,
    ) -> None:
        """Record formulas and merged ranges discovered in a physical row."""
        for col, cell_meta in enumerate(row_meta):
            if not cell_meta:
                continue
            if config.extract_formulas and "formula" in cell_meta:
                formulas.append(
                    RawFormula(
                        cell_ref=cell_ref(base_row, col),
                        formula_text=cell_meta["formula"],
                        display_value=cell_meta.get("display", ""),
                    )
                )
            span = cell_meta.get("span")
            if span:
                rows_spanned, cols_spanned = span
                merged.append(
                    range_ref(
                        base_row,
                        col,
                        base_row + max(rows_spanned, 1) - 1,
                        col + max(cols_spanned, 1) - 1,
                    )
                )

    # -- cell values ----------------------------------------------------------

    def _cell_value(self, cell: Any) -> Any:
        """Return the typed Python value for a ``table-cell`` (section 5.3)."""
        value_type = cell.getAttribute("valuetype")

        if value_type in ("float", "percentage", "currency"):
            return self._to_float(cell.getAttribute("value"))
        if value_type == "boolean":
            return self._to_bool(cell.getAttribute("booleanvalue"))
        if value_type == "date":
            return self._to_datelike(cell.getAttribute("datevalue"))
        if value_type == "time":
            return cell.getAttribute("timevalue") or self._cell_text(cell)
        # "string" or missing -> visible text.
        return self._cell_text(cell)

    def _cell_text(self, cell: Any) -> Any:
        """Concatenate the cell's ``text:p`` paragraphs (joined by newlines)."""
        paragraphs = cell.getElementsByType(odf.text.P)
        if paragraphs:
            parts = [odf.teletype.extractText(p) for p in paragraphs]
            text = "\n".join(parts)
            return text if text != "" else None
        text = odf.teletype.extractText(cell)
        return text if text else None

    def _cell_display_str(self, cell: Any) -> str:
        """Best-effort string display value for a formula cell."""
        value_type = cell.getAttribute("valuetype")
        if value_type in ("float", "percentage", "currency"):
            raw = cell.getAttribute("value")
            text = self._cell_text(cell)
            if text:
                return str(text)
            return "" if raw is None else str(self._to_float(raw))
        if value_type == "boolean":
            return str(self._to_bool(cell.getAttribute("booleanvalue")))
        if value_type == "date":
            return cell.getAttribute("datevalue") or ""
        text = self._cell_text(cell)
        return "" if text is None else str(text)

    @staticmethod
    def _to_float(raw: Any) -> Any:
        """Parse an ODS numeric ``office:value`` into ``float`` (``None`` on failure)."""
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_bool(raw: Any) -> Any:
        """Parse an ODS ``office:boolean-value`` into ``bool`` (``None`` on failure)."""
        if raw is None:
            return None
        if isinstance(raw, bool):
            return raw
        lowered = str(raw).strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        return None

    @staticmethod
    def _to_datelike(raw: Any) -> Any:
        """Parse an ODS ``office:date-value`` into ``date`` or ``datetime``.

        Returns a :class:`datetime.date` for date-only values and a
        :class:`datetime.datetime` when a time component is present. Falls back to
        the raw string if the value cannot be parsed.
        """
        if not raw:
            return raw or None
        text = str(raw)
        if "T" in text:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M"):
                try:
                    return datetime.strptime(text, fmt)
                except ValueError:
                    continue
            # ISO with timezone or other variants.
            try:
                return datetime.fromisoformat(text)
            except ValueError:
                return text
        try:
            return datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError:
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                return text
            return parsed.date() if parsed.time() == datetime.min.time() else parsed

    # -- named ranges / tables / metadata ------------------------------------

    def _parse_named_ranges(self, doc: Any, warnings: list[str]) -> list[RawNamedRange]:
        """Extract workbook-scoped named ranges (section 5.2)."""
        named: list[RawNamedRange] = []
        try:
            for nr in doc.spreadsheet.getElementsByType(odf.table.NamedRange):
                name = nr.getAttribute("name") or ""
                refers_to = nr.getAttribute("cellrangeaddress") or ""
                named.append(RawNamedRange(name=name, refers_to=refers_to, scope="workbook"))
        except Exception as exc:  # noqa: BLE001 - named ranges are best effort
            warnings.append(f"named-range extraction failed: {exc}")
        return named

    def _parse_formal_tables(self, tbl: Any, warnings: list[str]) -> list[RawTable]:
        """Best-effort formal table / database range extraction (section 5.4).

        ODS database ranges live on the spreadsheet, not the table; this is a best
        effort and an empty result is acceptable since inferred detection covers
        untabled ODS data. Returns ``[]`` if the elements are unavailable.
        """
        try:
            database_range = odf.table.DatabaseRange
        except AttributeError:
            return []

        tables: list[RawTable] = []
        try:
            for dr in tbl.getElementsByType(database_range):
                name = dr.getAttribute("name") or ""
                target = dr.getAttribute("targetrangeaddress") or ""
                tables.append(RawTable(name=name, range_ref=target))
        except Exception as exc:  # noqa: BLE001 - formal tables are best effort
            warnings.append(f"formal-table extraction failed: {exc}")
            return []
        return tables

    def _collect_table_style_display(self, doc: Any) -> dict[str, bool]:
        """Map automatic table style name -> visible flag (``table:display``)."""
        display: dict[str, bool] = {}
        try:
            import odf.style

            containers = []
            if getattr(doc, "automaticstyles", None) is not None:
                containers.append(doc.automaticstyles)
            if getattr(doc, "styles", None) is not None:
                containers.append(doc.styles)
            for container in containers:
                for style in container.getElementsByType(odf.style.Style):
                    style_name = style.getAttribute("name")
                    if not style_name:
                        continue
                    for props in style.getElementsByType(odf.style.TableProperties):
                        shown = props.getAttribute("display")
                        if shown is not None:
                            display[style_name] = str(shown).lower() != "false"
        except Exception:  # noqa: BLE001 - visibility detection is best effort
            return {}
        return display

    def _parse_properties(self, doc: Any) -> dict[str, Any]:
        """Extract a few easily-available document metadata fields (section 5.1)."""
        properties: dict[str, Any] = {}
        meta = getattr(doc, "meta", None)
        if meta is None:
            return properties
        try:
            import odf.dc
            import odf.meta

            for attr, element in (
                ("title", getattr(odf.dc, "Title", None)),
                ("creator", getattr(odf.dc, "Creator", None)),
                ("subject", getattr(odf.dc, "Subject", None)),
                ("description", getattr(odf.dc, "Description", None)),
                ("date", getattr(odf.dc, "Date", None)),
            ):
                if element is None:
                    continue
                found = meta.getElementsByType(element)
                if found:
                    text = odf.teletype.extractText(found[0])
                    if text:
                        properties[attr] = text
            generator = getattr(odf.meta, "Generator", None)
            if generator is not None:
                found = meta.getElementsByType(generator)
                if found:
                    text = odf.teletype.extractText(found[0])
                    if text:
                        properties["generator"] = text
        except Exception:  # noqa: BLE001 - metadata is best effort
            return properties
        return properties

    # -- low-level helpers ----------------------------------------------------

    @staticmethod
    def _local_name(node: Any) -> str:
        """Return the namespace-stripped local element name of ``node``."""
        qname = getattr(node, "qname", None)
        if isinstance(qname, tuple) and len(qname) == 2:
            return qname[1]
        return ""

    @staticmethod
    def _int_attr(node: Any, attr: str, *, default: int) -> int:
        """Read an integer attribute, falling back to ``default`` when absent/bad."""
        raw = node.getAttribute(attr)
        if raw is None:
            return default
        try:
            return int(raw)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _strip_trailing_none(values: list[Any]) -> list[Any]:
        """Drop trailing ``None`` entries so the used range stays bounded."""
        end = len(values)
        while end > 0 and values[end - 1] is None:
            end -= 1
        return values[:end]
