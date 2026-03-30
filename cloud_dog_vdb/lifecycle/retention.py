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


def should_purge(created_at_epoch: float, retention_days: int) -> bool:
    """Handle should purge."""
    return (time.time() - created_at_epoch) > max(0, retention_days) * 86400


def purge_candidates(records: list[dict], retention_days: int) -> list[dict]:
    """Handle purge candidates."""
    out: list[dict] = []
    for record in records:
        created = float(record.get("metadata", {}).get("created_at_epoch", 0) or 0)
        if created and should_purge(created, retention_days):
            out.append(record)
    return out
