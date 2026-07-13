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

"""Retrieval tuning for spreadsheet search (requirements sections 16, 5.19 Phase 3).

Builds backend-neutral :class:`cloud_dog_vdb.domain.models.SearchRequest` objects
for the spreadsheet retrieval views (conceptual / schema / factual / lineage),
carrying hybrid + lexical hints, identifier boosting, and query-language detection
for language-aware boosting. Actual hybrid execution depends on backend
capability; these hints let the search layer tune ranking per view.
"""

from __future__ import annotations

from cloud_dog_vdb.domain.models import SearchRequest
from cloud_dog_vdb.spreadsheet.i18n import detect_language

#: Object types surfaced by each retrieval view (section 16.2).
RETRIEVAL_VIEWS: dict[str, tuple[str, ...]] = {
    "conceptual": ("workbook", "sheet", "table", "column", "pivot"),
    "schema": ("column", "table"),
    "factual": ("row_batch", "named_range"),
    "lineage": ("workbook", "sheet", "table"),
}

#: Metadata fields used for lexical / exact-term matching (section 16.1).
LEXICAL_FIELDS = ("keyword_text", "table_name", "sheet_name", "title")


def detect_query_language(query_text: str) -> str:
    """Detect the language of a search query for language-aware boosting (section 5.19)."""
    return detect_language(query_text)


def build_search_request(
    query_text: str,
    *,
    view: str = "conceptual",
    sheet: str | None = None,
    table: str | None = None,
    object_type: str | None = None,
    language: str | None = None,
    top_k: int = 10,
    score_threshold: float | None = None,
) -> SearchRequest:
    """Build a spreadsheet-tuned search request for a retrieval view (section 16)."""
    if view not in RETRIEVAL_VIEWS:
        raise ValueError(f"unknown retrieval view: {view!r}")

    filters: dict[str, object] = {"source_type": "excel"}
    if sheet:
        filters["sheet_name"] = sheet
    if table:
        filters["table_name"] = table
    if object_type:
        filters["object_type"] = object_type

    query_language = language or detect_query_language(query_text)
    hints: dict[str, object] = {
        "retrieval_view": view,
        "object_types": list(RETRIEVAL_VIEWS[view]),
        "lexical_fields": list(LEXICAL_FIELDS),
        "boost_identifiers": True,
        "hybrid": True,
    }
    if query_language and query_language != "und":
        hints["query_language"] = query_language
        hints["boost_language_match"] = True

    return SearchRequest(
        query_text=query_text,
        top_k=top_k,
        filters=filters,
        include_metadata=True,
        score_threshold=score_threshold,
        query_plan_hints=hints,
    )
