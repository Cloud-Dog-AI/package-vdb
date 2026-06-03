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

import warnings

from cloud_dog_vdb.versioning.schema_version import SchemaVersionManager


def test_schema_versioning_tracks_and_plans_migrations():
    manager = SchemaVersionManager()
    v1 = manager.register(
        "docs",
        dimension_count=1024,
        metadata_fields=["tenant_id", "source_uri"],
        embedding_model="bge-m3",
    )
    assert v1.version == 1

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        mismatched = manager.warn_if_query_mismatch(
            "docs",
            expected_dimension_count=768,
            expected_embedding_model="nomic-embed-text",
        )
    assert mismatched is True
    assert captured

    plan = manager.plan_migration(
        "docs",
        dimension_count=768,
        metadata_fields=["tenant_id", "source_uri", "chunk_id"],
        embedding_model="nomic-embed-text",
    )
    assert plan.to_version >= 2
    assert plan.dimension_changed is True
    assert plan.reembed_required is True
