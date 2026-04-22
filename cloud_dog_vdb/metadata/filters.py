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

from dataclasses import dataclass, field
from typing import Any, Mapping

SCALAR_FILTER_FIELDS = (
    "tenant_id",
    "namespace",
    "app_id",
    "session_id",
    "doc_id",
    "record_id",
    "source_uri",
    "content_hash",
    "is_latest",
    "lifecycle_state",
)
EMPTY_VALUES = ("", None, [], (), {})


@dataclass(frozen=True, slots=True)
class MetadataFilter:
    """Portable metadata filter contract shared across backends."""

    tenant_id: str | None = None
    namespace: str | None = None
    app_id: str | None = None
    session_id: str | None = None
    doc_id: str | None = None
    record_id: str | None = None
    source_uri: str | None = None
    content_hash: str | None = None
    is_latest: bool | None = None
    lifecycle_state: str | None = None
    access_tags: tuple[str, ...] = field(default_factory=tuple)

    def to_backend_filter(self) -> dict[str, Any]:
        """Return the backend-safe scalar subset of the filter."""
        out: dict[str, Any] = {}
        for field_name in SCALAR_FILTER_FIELDS:
            value = getattr(self, field_name)
            if value not in EMPTY_VALUES:
                out[field_name] = value
        return out


def _coerce_access_tags(value: Any) -> tuple[str, ...]:
    if value in EMPTY_VALUES:
        return ()
    if isinstance(value, str):
        items = [value]
    else:
        items = list(value)
    return tuple(str(item).strip() for item in items if str(item).strip())


def coerce_metadata_filter(raw: MetadataFilter | Mapping[str, Any] | None) -> MetadataFilter:
    """Coerce raw filter input into the portable metadata filter contract."""
    if raw is None:
        return MetadataFilter()
    if isinstance(raw, MetadataFilter):
        return raw
    payload = dict(raw)
    kwargs: dict[str, Any] = {}
    for field_name in SCALAR_FILTER_FIELDS:
        if field_name in payload:
            kwargs[field_name] = payload[field_name]
    kwargs["access_tags"] = _coerce_access_tags(payload.get("access_tags"))
    return MetadataFilter(**kwargs)


def filter_to_backend_query(raw: MetadataFilter | Mapping[str, Any] | None) -> dict[str, Any]:
    """Translate a portable filter into the adapter-facing scalar query form."""
    return coerce_metadata_filter(raw).to_backend_filter()


def matches_metadata(metadata: Mapping[str, Any], raw: MetadataFilter | Mapping[str, Any] | None) -> bool:
    """Return whether metadata satisfies the portable filter contract."""
    spec = coerce_metadata_filter(raw)
    for field_name in SCALAR_FILTER_FIELDS:
        expected = getattr(spec, field_name)
        if expected in EMPTY_VALUES:
            continue
        if metadata.get(field_name) != expected:
            return False

    if spec.access_tags:
        actual = metadata.get("access_tags", [])
        actual_tags = {str(item).strip() for item in actual if str(item).strip()}
        if not set(spec.access_tags).issubset(actual_tags):
            return False
    return True
