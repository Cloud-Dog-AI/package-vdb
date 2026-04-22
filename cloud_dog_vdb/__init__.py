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

from cloud_dog_vdb.domain.models import (
    CapabilityDescriptor,
    CollectionSpec,
    Job,
    Record,
    SearchRequest,
    SearchResponse,
    SearchResult,
)
from cloud_dog_vdb.factory import get_vdb_client
from cloud_dog_vdb.ingestion.pipeline import IngestionPipeline, ParserIngestionOptions, ingest_document
from cloud_dog_vdb.runtime.client import VDBClient

__version__ = "0.5.2"

__all__ = [
    "VDBClient",
    "CapabilityDescriptor",
    "CollectionSpec",
    "Record",
    "SearchRequest",
    "SearchResult",
    "SearchResponse",
    "Job",
    "get_vdb_client",
    "ingest_document",
    "ParserIngestionOptions",
    "IngestionPipeline",
]
