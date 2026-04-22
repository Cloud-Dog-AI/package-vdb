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

from dataclasses import dataclass

from cloud_dog_vdb.capabilities.planner import plan_search
from cloud_dog_vdb.domain.models import CapabilityDescriptor, SearchRequest, SearchResponse


@dataclass(frozen=True, slots=True)
class SearchPlan:
    """Represent search plan."""

    mode: str
    top_k: int
    filters: dict


def build_search_plan(request: SearchRequest, capabilities: CapabilityDescriptor) -> SearchPlan:
    """Build search plan."""
    plan = plan_search(request, capabilities)
    return SearchPlan(mode=str(plan["mode"]), top_k=int(plan["top_k"]), filters=dict(plan["filters"]))


async def run_search(vdb_client, collection: str, request: SearchRequest) -> SearchResponse:
    """Execute a search request through the client.

    The runtime client already applies adapter-level planning; this helper keeps
    a stable seam for system/application flows and future instrumentation.
    """
    return await vdb_client.search(collection, request)
