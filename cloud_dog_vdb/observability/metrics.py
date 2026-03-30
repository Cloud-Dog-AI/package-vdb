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


@dataclass(slots=True)
class Metrics:
    """Represent metrics."""
    counters: dict[str, int] = field(default_factory=dict)
    gauges: dict[str, float] = field(default_factory=dict)

    def inc(self, key: str, amount: int = 1) -> None:
        """Handle inc."""
        self.counters[key] = self.counters.get(key, 0) + amount

    def set_gauge(self, key: str, value: float) -> None:
        """Handle set gauge."""
        self.gauges[key] = float(value)

    def snapshot(self) -> dict[str, dict[str, float | int]]:
        """Handle snapshot."""
        return {"counters": dict(self.counters), "gauges": dict(self.gauges)}
