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
from pathlib import Path

import pytest

from cloud_dog_vdb import CollectionSpec, SearchRequest, get_vdb_client
from cloud_dog_vdb.ingestion.pipeline import ParserIngestionOptions, ingest_document


@pytest.mark.asyncio
async def test_at2_1_parser_first_end_to_end(
    chroma_ready: dict,
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, str]],
) -> None:
    mineru = services["mineru"]
    if not mineru.get("enabled"):
        pytest.fail("MINERU_ENABLED must be true for AT2 parser-first flow", pytrace=False)
    base_url = str(mineru.get("base_url", "")).strip()
    if not base_url:
        pytest.fail("MINERU_BASE_URL missing for AT2 parser-first flow", pytrace=False)

    runtime = {
        "vector_stores": {
            "default_backend": "chroma",
            "chroma": {
                "enabled": True,
                "local_mode": False,
                "base_url": chroma_ready.get("base_url", ""),
                "auth_token": chroma_ready.get("auth_token", ""),
            },
        }
    }
    client = get_vdb_client(runtime)
    collection = f"cloud_dog_ai_at2_parser_{uuid.uuid4().hex[:8]}"
    package_root = Path(__file__).resolve().parents[3]
    source_name = str(corpus_entries[0]["file"])
    source_path = package_root / "test-data" / source_name
    try:
        await client.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        ids = await ingest_document(
            client,
            collection,
            source_path.read_bytes(),
            source_uri=f"file://{source_name}",
            options=ParserIngestionOptions(
                parser_chain=["mineru", "internal"],
                ocr_mode="disabled",
                table_policy="table_as_markdown",
            ),
            parser_services={
                "mineru": {
                    "base_url": base_url,
                    "api_key": str(mineru.get("api_key", "")),
                    "timeout_seconds": float(mineru.get("timeout_seconds", 120.0) or 120.0),
                }
            },
            metadata={"tenant_id": "at2_tenant", "source_type": "file"},
        )
        assert len(ids) >= 1
        records = await client.list_records(collection, {"tenant_id": "at2_tenant"})
        assert records
        assert str(records[0].metadata.get("parser_provider", "")) in {"mineru", "internal"}

        search = await client.search(
            collection,
            SearchRequest(query_text="*", top_k=5, filters={"tenant_id": "at2_tenant"}),
        )
        assert len(search.results) >= 1
    finally:
        await client.delete_collection(collection)
