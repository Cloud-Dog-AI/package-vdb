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

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping


def should_purge(created_at_epoch: float, retention_days: int) -> bool:
    """Legacy epoch-based retention check retained for compatibility."""
    return (time.time() - created_at_epoch) > max(0, retention_days) * 86400


def _parse_created_at(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def ttl_expired(record: Mapping[str, Any], *, now: datetime | None = None) -> bool:
    """Return whether canonical ttl_days retention has elapsed for a record."""
    metadata = record.get("metadata", record)
    if not isinstance(metadata, Mapping):
        return False
    ttl_days = metadata.get("ttl_days")
    if ttl_days in ("", None):
        return False
    try:
        ttl_days_value = int(ttl_days)
    except (TypeError, ValueError):
        return False
    created_at = _parse_created_at(metadata.get("created_at"))
    if created_at is None:
        return False
    if now is None:
        now = datetime.now(tz=timezone.utc)
    deadline = created_at + timedelta(days=max(ttl_days_value, 0))
    return now >= deadline


def purge_candidates(
    records: list[dict[str, Any]],
    retention_days: int | None = None,
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return records whose canonical ttl_days retention has elapsed."""
    out: list[dict] = []
    for record in records:
        metadata = record.get("metadata", record)
        if not isinstance(metadata, Mapping):
            continue
        effective_record: dict[str, Any]
        if retention_days is not None and metadata.get("ttl_days") in ("", None):
            effective_metadata = dict(metadata)
            effective_metadata["ttl_days"] = retention_days
            effective_record = dict(record)
            effective_record["metadata"] = effective_metadata
        else:
            effective_record = dict(record)
        if ttl_expired(effective_record, now=now):
            out.append(record)
    return out
