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
from datetime import datetime, timezone
from typing import Any

REQUIRED_FIELDS = {"tenant_id", "source_uri", "source_type", "lifecycle_state", "created_at"}
VALID_SOURCE_TYPES = {"web", "file", "api", "database", "other"}
VALID_LIFECYCLE_STATES = {"active", "deleted", "superseded", "archived"}
DEFAULT_MAX_METADATA_BYTES = 40960


def _is_rfc3339_utc(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.astimezone(timezone.utc).utcoffset().total_seconds() == 0.0


class MetadataValidator:
    """Represent metadata validator."""
    def __init__(self, *, max_metadata_bytes: int = DEFAULT_MAX_METADATA_BYTES) -> None:
        self.max_metadata_bytes = max_metadata_bytes

    def validate(self, metadata: dict[str, Any]) -> tuple[bool, str]:
        """Handle validate."""
        missing = [f for f in REQUIRED_FIELDS if f not in metadata]
        if missing:
            return False, f"missing: {', '.join(sorted(missing))}"
        if metadata.get("source_type") not in VALID_SOURCE_TYPES:
            return False, "invalid source_type"
        if metadata.get("lifecycle_state") not in VALID_LIFECYCLE_STATES:
            return False, "invalid lifecycle_state"

        created_at = str(metadata.get("created_at", ""))
        if not _is_rfc3339_utc(created_at):
            return False, "invalid created_at (must be UTC RFC3339)"

        size = len(json.dumps(metadata, sort_keys=True, default=str).encode("utf-8"))
        if size > self.max_metadata_bytes:
            return False, f"metadata_too_large:{size}"
        return True, "ok"


def validate_metadata(
    metadata: dict[str, Any], *, max_metadata_bytes: int = DEFAULT_MAX_METADATA_BYTES
) -> tuple[bool, str]:
    """Validate metadata."""
    return MetadataValidator(max_metadata_bytes=max_metadata_bytes).validate(metadata)
