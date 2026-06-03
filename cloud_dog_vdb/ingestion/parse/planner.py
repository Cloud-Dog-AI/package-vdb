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

from cloud_dog_vdb.ingestion.parse.registry import ParserRegistry


def pick_first_available(chain: list[str], registry: ParserRegistry) -> str | None:
    """Handle pick first available."""
    available = set(registry.list_ids())
    for provider_id in chain:
        if provider_id in available:
            return provider_id
    return None


def build_fallback_chain(chain: list[str], failed_provider: str) -> list[str]:
    """Build fallback chain."""
    return [provider_id for provider_id in chain if provider_id != failed_provider]
