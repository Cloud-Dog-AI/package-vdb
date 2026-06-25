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

import uuid

import pytest

from cloud_dog_vdb import CollectionSpec, get_vdb_client
from cloud_dog_vdb.ingestion.chunk.recursive import RecursiveChunker
from cloud_dog_vdb.ingestion.convert.pandas_conv import PandasConverter
from cloud_dog_vdb.ingestion.pipeline import ingest_text


@pytest.mark.asyncio
async def test_ingestion_real_backend_pattern(chroma_ready):
    cfg = chroma_ready
    collection = f"cloud_dog_ai_ingest_{uuid.uuid4().hex[:10]}"
    client_cfg = {
        "vector_stores": {
            "default_backend": "chroma",
            "chroma": {
                "enabled": True,
                "local_mode": False,
                "base_url": cfg.get("base_url", ""),
                "auth_token": cfg.get("auth_token", ""),
            },
        }
    }
    c = get_vdb_client(client_cfg)
    try:
        await c.delete_collection(collection)
        await c.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        ids = await ingest_text(c, collection, "hello\n\nworld", RecursiveChunker(), PandasConverter())
        assert len(ids) >= 1
    finally:
        await c.delete_collection(collection)
