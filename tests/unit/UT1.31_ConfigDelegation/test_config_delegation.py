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

import importlib

import pytest

from cloud_dog_vdb.adapters.chroma import ChromaAdapter
from cloud_dog_vdb.adapters.opensearch import OpenSearchAdapter
from cloud_dog_vdb.adapters.pgvector import PGVectorAdapter
from cloud_dog_vdb.adapters.qdrant import QdrantAdapter
from cloud_dog_vdb.adapters.weaviate import WeaviateAdapter
from cloud_dog_vdb.config.models import ProviderConfig


def test_adapters_use_preresolved_provider_config() -> None:
    chroma = ChromaAdapter(
        ProviderConfig(provider_id="chroma", base_url="https://chroma", api_key="k"), local_mode=True
    )
    qdrant = QdrantAdapter(ProviderConfig(provider_id="qdrant", base_url="https://qdrant", api_key="k2"))
    weaviate = WeaviateAdapter(ProviderConfig(provider_id="weaviate", base_url="https://weaviate", api_key="k3"))
    opensearch = OpenSearchAdapter(
        ProviderConfig(provider_id="opensearch", base_url="", host="os", port=9200, username="u", password="p")
    )

    assert chroma._headers()["Authorization"] == "Bearer k"
    assert qdrant._headers()["api-key"] == "k2"
    assert weaviate._headers()["Authorization"] == "Bearer k3"
    assert opensearch._auth() == ("u", "p")
    assert opensearch._base() == "http://os:9200"

    for adapter in (chroma, qdrant, weaviate, opensearch):
        assert not hasattr(adapter, "_runtime")


@pytest.mark.asyncio
async def test_pgvector_connection_uses_config_database_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, str] = {}

    class _Conn:
        async def close(self) -> None:
            return None

    async def _fake_connect(uri: str):
        seen["uri"] = uri
        return _Conn()

    monkeypatch.setattr("cloud_dog_vdb.adapters.pgvector.asyncpg.connect", _fake_connect)
    adapter = PGVectorAdapter(ProviderConfig(provider_id="pgvector", database_uri="postgresql://db/test"))
    conn = await adapter._conn()
    await conn.close()

    assert seen["uri"] == "postgresql://db/test"
    assert not hasattr(adapter, "_runtime")


def test_local_secrets_module_is_removed() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("cloud_dog_vdb.secrets")
