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

import uuid
from collections.abc import Sequence
from typing import Any, Mapping

from cloud_dog_vdb import CollectionSpec, Record, VDBClient


async def assert_metadata_field_parity(
    client: VDBClient,
    *,
    provider_ids: Sequence[str],
    metadata: Mapping[str, Any],
    filters: Mapping[str, Any] | None = None,
) -> set[str]:
    """Run the same metadata operation across multiple backends and assert identical field sets."""
    observed: dict[str, set[str]] = {}

    for provider_id in provider_ids:
        collection = f"cloud_dog_ai_meta_parity_{provider_id}_{uuid.uuid4().hex[:8]}"
        await client.create_collection(
            CollectionSpec(name=collection, namespace="parity", embedding_dim=4),
            provider_id=provider_id,
        )
        try:
            await client.upsert_records(
                collection,
                [
                    Record(
                        record_id=f"{provider_id}-record",
                        content="metadata parity payload",
                        metadata=dict(metadata),
                    )
                ],
                provider_id=provider_id,
            )
            rows = await client.list_records(
                collection,
                filters=filters,
                paging={"offset": 0, "limit": 10},
                provider_id=provider_id,
            )
            assert rows, f"no rows returned for provider {provider_id}"
            observed[provider_id] = set(rows[0].metadata.keys())
        finally:
            await client.delete_collection(collection, provider_id=provider_id)

    baseline_provider = next(iter(observed))
    baseline_fields = observed[baseline_provider]
    for provider_id, fields in observed.items():
        assert fields == baseline_fields, (
            f"metadata field mismatch for {provider_id}: expected {sorted(baseline_fields)} got {sorted(fields)}"
        )
    return baseline_fields
