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

from cloud_dog_vdb.domain.models import CollectionSpec


def validate_spec(spec: CollectionSpec) -> None:
    """Validate spec."""
    if not spec.name.strip():
        raise ValueError("collection name is required")
    if spec.embedding_dim <= 0:
        raise ValueError("embedding_dim must be positive")


def spec_key(spec: CollectionSpec) -> str:
    """Handle spec key."""
    namespace = spec.namespace.strip()
    return f"{namespace}:{spec.name}" if namespace else spec.name


__all__ = ["CollectionSpec", "validate_spec", "spec_key"]
