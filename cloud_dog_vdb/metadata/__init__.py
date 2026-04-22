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

# cloud_dog_vdb metadata

from cloud_dog_vdb.metadata.identity import (
    compose_chunk_metadata,
    compute_content_hash,
    compute_doc_id,
    compute_record_id,
    compute_source_hash,
    normalise_source_uri,
)
from cloud_dog_vdb.metadata.filters import MetadataFilter, coerce_metadata_filter, filter_to_backend_query, matches_metadata
from cloud_dog_vdb.metadata.provenance import ProvenancePatch, build_provenance_patch, merge_provenance
from cloud_dog_vdb.metadata.schema import CanonicalMetadata, MetadataValidator, validate_metadata

__all__ = [
    "CanonicalMetadata",
    "MetadataFilter",
    "MetadataValidator",
    "ProvenancePatch",
    "build_provenance_patch",
    "coerce_metadata_filter",
    "compose_chunk_metadata",
    "compute_content_hash",
    "compute_doc_id",
    "compute_record_id",
    "compute_source_hash",
    "filter_to_backend_query",
    "matches_metadata",
    "merge_provenance",
    "normalise_source_uri",
    "validate_metadata",
]
