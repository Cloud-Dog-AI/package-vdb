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

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

RFC3339_UTC_SUFFIXES = ("Z", "+00:00")
HEX_RE = re.compile(r"^[0-9a-fA-F]+$")
MAX_METADATA_BYTES = 64 * 1024
REQUIRED_FIELDS = {"doc_id", "source_uri", "lifecycle_state", "created_at"}
VALID_SOURCE_TYPES = {"web", "file", "api", "database", "other"}
VALID_LIFECYCLE_STATES = {"active", "deleted", "superseded", "archived"}


def _is_rfc3339_utc(value: str) -> bool:
    if not value or "T" not in value or not value.endswith(RFC3339_UTC_SUFFIXES):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.astimezone(timezone.utc).utcoffset().total_seconds() == 0.0


@dataclass(slots=True)
class CanonicalMetadata:
    """Represent the canonical metadata contract for stored VDB records."""

    doc_id: str = ""
    record_id: str = ""
    chunk_id: str = ""
    chunk_index: int | None = None
    supersedes: str | None = None
    is_latest: bool | None = None
    tenant_id: str = ""
    namespace: str = ""
    source_uri: str = ""
    source_type: str = ""
    filename: str = ""
    mime_type: str = ""
    size_bytes: int | None = None
    content_hash: str = ""
    source_hash: str = ""
    created_at: str = ""
    ingested_at: str = ""
    lifecycle_state: str = ""
    ttl_days: int | None = None
    app_id: str = ""
    user_id: str = ""
    session_id: str = ""
    profile: str = ""
    collection: str = ""
    embedding_model: str = ""
    embedding_dim: int | None = None
    chunker: str = ""
    chunker_version: str = ""
    token_count: int | None = None
    access_tags: list[str] = field(default_factory=list)
    pii_present: bool | None = None
    parser_name: str = ""
    parser_version: str = ""
    parser_provider: str = ""
    ocr_provider: str = ""
    ocr_engine: str = ""
    ocr_applied: bool | None = None
    ocr_confidence: float | None = None
    page: int | None = None
    page_number: int | None = None
    page_range: str = ""
    section: str = ""
    section_path: str = ""
    table_id: str = ""
    table_title: str = ""
    chunk_kind: str = ""
    extras: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, metadata: Mapping[str, Any]) -> "CanonicalMetadata":
        """Build canonical metadata from a plain mapping."""
        payload = dict(metadata)
        field_names = cls.__dataclass_fields__.keys()
        known = {name: payload.pop(name) for name in list(payload) if name in field_names and name != "extras"}
        return cls(**known, extras=payload)

    def to_dict(self) -> dict[str, Any]:
        """Convert metadata back to a plain dict."""
        payload = asdict(self)
        extras = payload.pop("extras", {})
        merged = {**payload, **extras}
        return {key: value for key, value in merged.items() if value not in ("", None, [], {})}


class MetadataValidator:
    """Represent metadata validator."""

    def __init__(self, *, max_metadata_bytes: int = MAX_METADATA_BYTES) -> None:
        self.max_metadata_bytes = max_metadata_bytes

    def validate(self, metadata: CanonicalMetadata | Mapping[str, Any]) -> list[str]:
        """Handle validate."""
        meta = metadata if isinstance(metadata, CanonicalMetadata) else CanonicalMetadata.from_mapping(metadata)
        payload = meta.to_dict()
        errors: list[str] = []

        missing = [field_name for field_name in REQUIRED_FIELDS if not str(payload.get(field_name, "")).strip()]
        if missing:
            errors.append(f"missing: {', '.join(sorted(missing))}")

        source_type = str(payload.get("source_type", "")).strip()
        if source_type and source_type not in VALID_SOURCE_TYPES:
            errors.append("invalid source_type")

        lifecycle_state = str(payload.get("lifecycle_state", "")).strip()
        if lifecycle_state and lifecycle_state not in VALID_LIFECYCLE_STATES:
            errors.append("invalid lifecycle_state")

        created_at = str(payload.get("created_at", "")).strip()
        if created_at and not _is_rfc3339_utc(created_at):
            errors.append("invalid created_at (must be UTC RFC3339)")

        ingested_at = str(payload.get("ingested_at", "")).strip()
        if ingested_at and not _is_rfc3339_utc(ingested_at):
            errors.append("invalid ingested_at (must be UTC RFC3339)")

        for field_name in ("content_hash", "source_hash"):
            value = str(payload.get(field_name, "")).strip()
            if value and not HEX_RE.fullmatch(value):
                errors.append(f"invalid {field_name} (must be hex)")

        ocr_confidence = payload.get("ocr_confidence")
        if ocr_confidence is not None:
            try:
                confidence_value = float(ocr_confidence)
            except (TypeError, ValueError):
                errors.append("invalid ocr_confidence")
            else:
                if confidence_value < 0.0 or confidence_value > 1.0:
                    errors.append("invalid ocr_confidence")

        size = len(json.dumps(payload, sort_keys=True, default=str).encode("utf-8"))
        if size >= self.max_metadata_bytes:
            errors.append(f"metadata_too_large:{size}")
        return errors


def validate_metadata(
    metadata: CanonicalMetadata | Mapping[str, Any], *, max_metadata_bytes: int = MAX_METADATA_BYTES
) -> list[str]:
    """Validate metadata."""
    return MetadataValidator(max_metadata_bytes=max_metadata_bytes).validate(metadata)
