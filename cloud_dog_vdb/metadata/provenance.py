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

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

PROVENANCE_FIELDS = (
    "parser_provider",
    "parser_version",
    "ocr_engine",
    "ocr_confidence",
    "page",
    "section",
    "table_id",
    "chunk_kind",
)
PROVENANCE_ALIASES = {
    "ocr_provider": "ocr_engine",
    "page_number": "page",
    "section_path": "section",
}
EMPTY_VALUES = ("", None, [], (), {})


@dataclass(frozen=True, slots=True)
class ProvenancePatch:
    """Additive provenance patch for parser/OCR/structure metadata."""

    parser_provider: str = ""
    parser_version: str = ""
    ocr_engine: str = ""
    ocr_confidence: float | None = None
    page: int | None = None
    section: str = ""
    table_id: str = ""
    chunk_kind: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return the non-empty patch payload."""
        return {key: value for key, value in asdict(self).items() if value not in EMPTY_VALUES}


def build_provenance_patch(**kwargs: Any) -> dict[str, Any]:
    """Build a normalised provenance patch."""
    return ProvenancePatch(**kwargs).to_dict()


def merge_provenance(
    metadata: Mapping[str, Any],
    provenance: ProvenancePatch | Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Merge provenance additively without overwriting existing non-empty values."""
    merged = dict(metadata)

    # Normalise any legacy aliases already present in the metadata.
    for alias, canonical in PROVENANCE_ALIASES.items():
        alias_value = merged.get(alias)
        canonical_value = merged.get(canonical)
        if canonical_value in EMPTY_VALUES and alias_value not in EMPTY_VALUES:
            merged[canonical] = alias_value
        if alias_value in EMPTY_VALUES and canonical_value not in EMPTY_VALUES:
            merged[alias] = canonical_value

    patch: dict[str, Any] = {}
    if provenance is not None:
        if isinstance(provenance, ProvenancePatch):
            patch.update(provenance.to_dict())
        else:
            patch.update(dict(provenance))
    patch.update(kwargs)

    normalised_patch: dict[str, Any] = {}
    for key, value in patch.items():
        canonical = PROVENANCE_ALIASES.get(key, key)
        if canonical in PROVENANCE_FIELDS:
            normalised_patch[canonical] = value

    for field_name, value in normalised_patch.items():
        if value in EMPTY_VALUES:
            continue
        if merged.get(field_name) in EMPTY_VALUES:
            merged[field_name] = value

    for alias, canonical in PROVENANCE_ALIASES.items():
        if merged.get(alias) in EMPTY_VALUES and merged.get(canonical) not in EMPTY_VALUES:
            merged[alias] = merged[canonical]

    return merged
