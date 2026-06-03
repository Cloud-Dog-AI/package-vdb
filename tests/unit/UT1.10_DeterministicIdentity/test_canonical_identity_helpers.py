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

from cloud_dog_vdb.metadata.identity import (
    compose_chunk_metadata,
    compute_doc_id,
    compute_source_hash,
    compute_record_id,
    normalise_source_uri,
)
from cloud_dog_vdb.metadata.schema import CanonicalMetadata


def test_normalise_source_uri_preserves_path_case_and_strips_trailing_slash() -> None:
    assert normalise_source_uri(" HTTP://Example.COM/Folder/File.PDF/ ") == "http://example.com/Folder/File.PDF"


def test_compute_source_hash_and_record_id_are_deterministic() -> None:
    source_hash = compute_source_hash(b"hello")
    assert len(source_hash) == 64
    doc_id = compute_doc_id("https://example.test/doc", source_hash)
    assert compute_record_id(doc_id, 7) == compute_record_id(doc_id, 7)


def test_compose_chunk_metadata_builds_chunk_specific_fields() -> None:
    doc_meta = CanonicalMetadata(
        source_uri="https://example.test/doc",
        source_hash="a" * 64,
        lifecycle_state="active",
        created_at="2026-04-10T12:00:00Z",
        filename="doc.pdf",
        mime_type="application/pdf",
    )
    chunk_meta = compose_chunk_metadata(doc_meta, 3, "chunk body", 42)
    assert chunk_meta.doc_id == compute_doc_id(doc_meta.source_uri, doc_meta.source_hash)
    assert chunk_meta.record_id == compute_record_id(chunk_meta.doc_id, 3)
    assert chunk_meta.chunk_index == 3
    assert chunk_meta.token_count == 42
    assert chunk_meta.filename == "doc.pdf"
