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

"""Backend-neutral Excel / spreadsheet indexing for cloud_dog_vdb (W28E-604).

This subpackage turns a spreadsheet workbook (``.xlsx``/``.xlsm``/``.ods``) into a
canonical multi-granularity object model (section 8) and a list of backend-neutral
:class:`cloud_dog_vdb.domain.models.Record` objects ready for upsert through the
existing :class:`cloud_dog_vdb.runtime.client.VDBClient`. It is pure and stateless:
the hosting service owns the SQL control plane and the actual backend upsert.

Macros and active content are never executed (section 17.1).
"""

from __future__ import annotations

from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.model import (
    ColumnNode,
    FormulaNode,
    NamedRangeNode,
    ObjectManifestEntry,
    PivotNode,
    RangeNode,
    RowBatchNode,
    SearchableRecord,
    SheetNode,
    TableNode,
    WorkbookExtraction,
    WorkbookNode,
)
from cloud_dog_vdb.spreadsheet.formats import (
    records_to_jsonl,
    table_to_csv,
    table_to_parquet,
    workbook_to_json,
    workbook_to_json_str,
)
from cloud_dog_vdb.spreadsheet.i18n import detect_language, dominant_language, translated_aliases
from cloud_dog_vdb.spreadsheet.parser.base import RawWorkbook, UnsupportedFormatError, detect_format
from cloud_dog_vdb.spreadsheet.pipeline import extract_workbook, searchable_records_to_records
from cloud_dog_vdb.spreadsheet.query import ExtractionQuery, ExtractionQueryResult, run_query
from cloud_dog_vdb.spreadsheet.retrieval import build_search_request, detect_query_language
from cloud_dog_vdb.spreadsheet.sensitivity import SensitivityPolicy

__all__ = [
    "SpreadsheetConfig",
    "WorkbookExtraction",
    "WorkbookNode",
    "SheetNode",
    "TableNode",
    "RangeNode",
    "ColumnNode",
    "RowBatchNode",
    "FormulaNode",
    "PivotNode",
    "NamedRangeNode",
    "SearchableRecord",
    "ObjectManifestEntry",
    "RawWorkbook",
    "UnsupportedFormatError",
    "detect_format",
    "extract_workbook",
    "searchable_records_to_records",
    "ExtractionQuery",
    "ExtractionQueryResult",
    "run_query",
    "workbook_to_json",
    "workbook_to_json_str",
    "records_to_jsonl",
    "table_to_csv",
    "table_to_parquet",
    "detect_language",
    "dominant_language",
    "translated_aliases",
    "SensitivityPolicy",
    "build_search_request",
    "detect_query_language",
]
