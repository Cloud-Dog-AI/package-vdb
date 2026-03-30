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


class IsolationManager:
    """Tenant isolation helper for record filtering and namespace keys."""

    @staticmethod
    def enforce_tenant(records: list[dict], tenant_id: str) -> list[dict]:
        """Handle enforce tenant."""
        return [r for r in records if r.get("metadata", {}).get("tenant_id") == tenant_id]

    @staticmethod
    def namespace_for(tenant_id: str, base_namespace: str = "") -> str:
        """Handle namespace for."""
        tenant = tenant_id.strip()
        base = base_namespace.strip()
        return f"{base}:{tenant}" if base else tenant


def enforce_tenant(records: list[dict], tenant_id: str) -> list[dict]:
    """Compatibility wrapper for existing imports."""
    return IsolationManager.enforce_tenant(records, tenant_id)
