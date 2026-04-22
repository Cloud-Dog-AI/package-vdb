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

from cloud_dog_vdb.metadata.schema import validate_metadata


def test_metadata_required_fields_and_enums():
    errors = validate_metadata(
        {
            "doc_id": "a" * 64,
            "source_uri": "u",
            "source_type": "web",
            "lifecycle_state": "active",
            "created_at": "2026-01-01T00:00:00Z",
        }
    )
    assert errors == []


def test_metadata_rejects_non_utc_rfc3339():
    errors = validate_metadata(
        {
            "doc_id": "a" * 64,
            "source_uri": "u",
            "source_type": "web",
            "lifecycle_state": "active",
            "created_at": "2026-01-01 00:00:00",
        }
    )
    assert any("created_at" in error for error in errors)


def test_metadata_enforces_max_size():
    errors = validate_metadata(
        {
            "doc_id": "a" * 64,
            "source_uri": "u",
            "source_type": "web",
            "lifecycle_state": "active",
            "created_at": "2026-01-01T00:00:00Z",
            "blob": "x" * 512,
        },
        max_metadata_bytes=128,
    )
    assert any(error.startswith("metadata_too_large:") for error in errors)
