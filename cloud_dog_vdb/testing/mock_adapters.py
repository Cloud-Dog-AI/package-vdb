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

from cloud_dog_vdb.adapters.chroma import ChromaAdapter
from cloud_dog_vdb.config.models import ProviderConfig


def mock_adapter(provider_id: str = "chroma"):
    """Handle mock adapter."""
    return ChromaAdapter(ProviderConfig(provider_id=provider_id), local_mode=True)


def mock_adapter_registry():
    """Handle mock adapter registry."""
    from cloud_dog_vdb.adapters.registry import AdapterRegistry

    registry = AdapterRegistry()
    registry.register("chroma", mock_adapter("chroma"))
    return registry
