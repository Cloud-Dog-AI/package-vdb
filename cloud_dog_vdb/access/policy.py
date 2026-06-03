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


@dataclass(frozen=True, slots=True)
class AccessPolicy:
    """Represent access policy."""

    readers: set[str] = field(default_factory=set)
    writers: set[str] = field(default_factory=set)
    admins: set[str] = field(default_factory=set)

    def can_read(self, role: str) -> bool:
        """Handle can read."""
        return role in self.readers or self.can_write(role)

    def can_write(self, role: str) -> bool:
        """Handle can write."""
        return role in self.writers or self.can_admin(role)

    def can_admin(self, role: str) -> bool:
        """Handle can admin."""
        return role in self.admins
