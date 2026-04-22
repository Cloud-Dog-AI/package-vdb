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

import hashlib
from dataclasses import replace
from typing import Any

from cloud_dog_vdb.metadata.normalise import normalise_source_uri
from cloud_dog_vdb.metadata.schema import CanonicalMetadata


def compute_doc_id(source_uri: str, content_hash: str) -> str:
    """Handle compute doc id."""
    raw = f"{normalise_source_uri(source_uri)}|{content_hash.strip().lower()}".encode()
    return hashlib.sha256(raw).hexdigest()


def compute_content_hash(text: str) -> str:
    """Handle compute content hash."""
    return hashlib.sha256(text.strip().encode()).hexdigest()


def compute_source_hash(source_bytes: bytes) -> str:
    """Handle compute source hash."""
    return hashlib.sha256(source_bytes).hexdigest()


def compute_record_id(doc_id: str, chunk_index: int | str, *legacy_parts: str) -> str:
    """Handle compute record id."""
    if legacy_parts:
        raw = "|".join([doc_id, str(chunk_index), *[str(part) for part in legacy_parts]]).encode()
        return hashlib.sha256(raw).hexdigest()
    raw = f"{doc_id}|{chunk_index}".encode()
    return hashlib.sha256(raw).hexdigest()


def compose_chunk_metadata(
    doc_meta: CanonicalMetadata | dict[str, Any], chunk_index: int, chunk_text: str, token_count: int
) -> CanonicalMetadata:
    """Assemble per-chunk metadata from document-level metadata."""
    base = doc_meta if isinstance(doc_meta, CanonicalMetadata) else CanonicalMetadata.from_mapping(doc_meta)
    chunk_hash = compute_content_hash(chunk_text)
    source_hash = base.source_hash or base.content_hash or chunk_hash
    doc_id = base.doc_id or compute_doc_id(base.source_uri, source_hash)
    chunk_id = str(base.chunk_id or chunk_index)
    record_id = compute_record_id(doc_id, chunk_index)
    metadata = replace(
        base,
        doc_id=doc_id,
        record_id=record_id,
        chunk_id=chunk_id,
        chunk_index=chunk_index,
        content_hash=chunk_hash,
        source_hash=source_hash,
        token_count=token_count,
        is_latest=True if base.is_latest is None else base.is_latest,
    )
    metadata.extras = dict(base.extras)
    return metadata
