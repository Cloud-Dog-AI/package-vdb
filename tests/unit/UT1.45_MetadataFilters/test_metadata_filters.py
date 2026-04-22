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

from cloud_dog_vdb.metadata.filters import MetadataFilter, coerce_metadata_filter, filter_to_backend_query, matches_metadata


def test_filter_translation_excludes_access_tags_from_backend_query() -> None:
    query = filter_to_backend_query(
        MetadataFilter(tenant_id="tenant-a", namespace="ns-a", lifecycle_state="active", access_tags=("finance",))
    )
    assert query == {"tenant_id": "tenant-a", "namespace": "ns-a", "lifecycle_state": "active"}


def test_filter_matches_access_tag_containment() -> None:
    metadata = {"tenant_id": "tenant-a", "access_tags": ["finance", "legal"], "lifecycle_state": "active"}
    assert matches_metadata(metadata, {"tenant_id": "tenant-a", "access_tags": ["finance"]}) is True
    assert matches_metadata(metadata, {"tenant_id": "tenant-a", "access_tags": ["finance", "ops"]}) is False


def test_filter_coercion_preserves_scalar_contract() -> None:
    spec = coerce_metadata_filter({"session_id": "s-1", "is_latest": True, "access_tags": ["a", "b"]})
    assert spec == MetadataFilter(session_id="s-1", is_latest=True, access_tags=("a", "b"))
