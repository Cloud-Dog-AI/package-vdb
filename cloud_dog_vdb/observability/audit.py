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

from datetime import datetime, timezone
from typing import Any


def audit_event(
    action: str, target: str, *, actor: str = "system", outcome: str = "success", details: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Handle audit event."""
    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "action": action,
        "target": target,
        "actor": actor,
        "outcome": outcome,
        "details": details or {},
    }
