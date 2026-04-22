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

from cloud_dog_vdb.metadata.schema import CanonicalMetadata, validate_metadata


def test_canonical_metadata_accepts_required_fields() -> None:
    errors = validate_metadata(
        CanonicalMetadata(
            doc_id="a" * 64,
            source_uri="https://example.test/playgroup/test-project.git",
            lifecycle_state="active",
            created_at="2026-04-10T12:00:00Z",
            namespace="tenant-a",
            ocr_engine="tesseract",
            ocr_confidence=0.98,
            page=1,
            section="intro",
        )
    )
    assert errors == []


def test_canonical_metadata_rejects_missing_required_fields() -> None:
    errors = validate_metadata({"source_uri": "https://example.test/doc", "lifecycle_state": "active"})
    assert any(error.startswith("missing:") for error in errors)
    assert any("created_at" in error or "doc_id" in error for error in errors)


def test_canonical_metadata_rejects_invalid_hash_and_ingested_at() -> None:
    errors = validate_metadata(
        {
            "doc_id": "b" * 64,
            "source_uri": "https://example.test/doc",
            "lifecycle_state": "active",
            "created_at": "2026-04-10T12:00:00Z",
            "ingested_at": "2026-04-10 12:00:00",
            "content_hash": "xyz",
            "source_hash": "nothex",
        }
    )
    assert "invalid ingested_at (must be UTC RFC3339)" in errors
    assert "invalid content_hash (must be hex)" in errors
    assert "invalid source_hash (must be hex)" in errors


def test_canonical_metadata_rejects_invalid_ocr_confidence() -> None:
    errors = validate_metadata(
        {
            "doc_id": "c" * 64,
            "source_uri": "https://example.test/doc",
            "lifecycle_state": "active",
            "created_at": "2026-04-10T12:00:00Z",
            "ocr_confidence": 1.5,
        }
    )
    assert "invalid ocr_confidence" in errors
