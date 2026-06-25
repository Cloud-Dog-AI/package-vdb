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

from cloud_dog_vdb.adapters.chroma import ChromaAdapter
from cloud_dog_vdb.adapters.opensearch import OpenSearchAdapter
from cloud_dog_vdb.adapters.pgvector import PGVectorAdapter
from cloud_dog_vdb.adapters.qdrant import QdrantAdapter
from cloud_dog_vdb.adapters.weaviate import WeaviateAdapter
from cloud_dog_vdb.config.models import ProviderConfig


def test_adapter_lifecycle_uses_supplied_config_without_env_overlay(monkeypatch):
    monkeypatch.setenv("CLOUD_DOG_VDB__CHROMA__API_KEY", "env-override-should-not-apply")

    chroma = ChromaAdapter(
        ProviderConfig(provider_id="chroma", base_url="https://chroma.local", api_key="cfg-key"), local_mode=True
    )
    qdrant = QdrantAdapter(ProviderConfig(provider_id="qdrant", base_url="https://qdrant.local", api_key="q-key"))
    weaviate = WeaviateAdapter(
        ProviderConfig(provider_id="weaviate", base_url="https://weaviate.local", api_key="w-key")
    )
    opensearch = OpenSearchAdapter(
        ProviderConfig(
            provider_id="opensearch", base_url="", host="os.local", port=9200, username="usr", password="pwd"
        )
    )
    pgvector = PGVectorAdapter(ProviderConfig(provider_id="pgvector", database_uri="postgresql://cfg-uri"))

    assert chroma._headers()["Authorization"] == "Bearer cfg-key"
    assert qdrant._headers()["api-key"] == "q-key"
    assert weaviate._headers()["Authorization"] == "Bearer w-key"
    assert opensearch._auth() == ("usr", "pwd")
    assert opensearch._base() == "http://os.local:9200"
    assert pgvector.config.database_uri == "postgresql://cfg-uri"

    assert "env-override-should-not-apply" not in str(chroma._headers())
