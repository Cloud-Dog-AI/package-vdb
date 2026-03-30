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

from cloud_dog_vdb.metadata.normalise import normalise_source_uri


def compute_doc_id(tenant_id: str, source_uri: str) -> str:
    """Handle compute doc id."""
    raw = f"{tenant_id}|{normalise_source_uri(source_uri)}".encode()
    return hashlib.sha256(raw).hexdigest()


def compute_content_hash(text: str) -> str:
    """Handle compute content hash."""
    return hashlib.sha256(text.strip().encode()).hexdigest()


def compute_record_id(doc_id: str, content_hash: str, chunk_id: str, embedding_model: str, chunker_version: str) -> str:
    """Handle compute record id."""
    raw = f"{doc_id}|{content_hash}|{chunk_id}|{embedding_model}|{chunker_version}".encode()
    return hashlib.sha256(raw).hexdigest()
