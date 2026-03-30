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

from cloud_dog_vdb.domain.models import CollectionSpec, Record


def sample_collection(name: str = "docs") -> CollectionSpec:
    """Handle sample collection."""
    return CollectionSpec(name=name, namespace="test", embedding_dim=4)


def sample_records() -> list[Record]:
    """Handle sample records."""
    return [
        Record(record_id="r1", content="hello", metadata={"tenant_id": "t1", "source_uri": "file://a"}),
        Record(record_id="r2", content="world", metadata={"tenant_id": "t1", "source_uri": "file://b"}),
    ]
