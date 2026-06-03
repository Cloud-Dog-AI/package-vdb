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

from cloud_dog_vdb.access.policy import AccessPolicy


def can_read(role: str, policy: AccessPolicy) -> bool:
    """Handle can read."""
    return role in policy.readers or role in policy.writers or role in policy.admins


def can_write(role: str, policy: AccessPolicy) -> bool:
    """Handle can write."""
    return role in policy.writers or role in policy.admins


def can_admin(role: str, policy: AccessPolicy) -> bool:
    """Handle can admin."""
    return role in policy.admins
