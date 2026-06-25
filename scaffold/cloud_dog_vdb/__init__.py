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

# cloud_dog_vdb — PS-60 Vector DB Interfaces for Cloud-Dog services
"""
Public API for cloud_dog_vdb.

Provides pluggable VDB backend adapters, canonical metadata enforcement,
deterministic identity, ingestion pipelines, job control, capability-aware
query planning, and lifecycle management.
"""

__version__ = "0.1.0"

# Public API will be exported here after implementation:
# from cloud_dog_vdb.adapters.registry import AdapterRegistry
# from cloud_dog_vdb.domain.models import CollectionSpec, Record, SearchRequest, SearchResponse
# from cloud_dog_vdb.ingestion.pipeline import IngestionPipeline
# from cloud_dog_vdb.collections.manager import CollectionManager
