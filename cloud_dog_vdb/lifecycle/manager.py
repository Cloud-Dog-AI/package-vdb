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

from typing import Any, Mapping

PURGEABLE_STATES = {"deleted", "superseded", "archived"}


def _coerce_record(record: Mapping[str, Any] | str) -> dict[str, Any]:
    if isinstance(record, Mapping):
        return dict(record)
    return {"record_id": str(record)}


def mark_deleted(record: Mapping[str, Any] | str) -> dict[str, Any]:
    """Mark a record as deleted."""
    out = _coerce_record(record)
    out["lifecycle_state"] = "deleted"
    out["is_latest"] = False
    return out


def mark_superseded(record: Mapping[str, Any] | str, new_record_id: str | None = None) -> dict[str, Any]:
    """Mark a record as superseded and optionally link the replacement record."""
    out = _coerce_record(record)
    out["lifecycle_state"] = "superseded"
    out["is_latest"] = False
    if new_record_id:
        out["supersedes"] = str(new_record_id)
    return out


def check_purge_safety(record: Mapping[str, Any] | str) -> bool:
    """Return whether a record is already in a purgeable lifecycle state."""
    if not isinstance(record, Mapping):
        return False
    lifecycle_state = str(record.get("lifecycle_state", "")).strip().lower()
    if lifecycle_state not in PURGEABLE_STATES:
        return False
    return bool(record.get("is_latest") is False)
