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

import pytest

from cloud_dog_vdb.adapters.chroma import ChromaAdapter
from cloud_dog_vdb.adapters.qdrant import QdrantAdapter
from cloud_dog_vdb.runtime.factory import build_runtime_client, provider_config_with_embeddings

EMBEDDING_BASE_URL = "https://embedding.example.invalid"


def test_runtime_factory_threads_embedding_config_into_provider_config() -> None:
    provider = provider_config_with_embeddings(
        "qdrant",
        {"enabled": True, "base_url": "https://qdrant.test"},
        {
            "provider": "ollama",
            "ollama": {
                "base_url": EMBEDDING_BASE_URL,
                "model": "nomic-embed-text",
                "timeout_seconds": 60,
            },
        },
    )
    assert provider.embedding_provider_id == "ollama"
    assert provider.embedding_base_url == EMBEDDING_BASE_URL
    assert provider.embedding_model == "nomic-embed-text"


@pytest.mark.asyncio
async def test_qdrant_adapter_uses_real_embedding_provider_when_configured() -> None:
    adapter = QdrantAdapter(
        provider_config_with_embeddings(
            "qdrant",
            {"enabled": True, "base_url": "https://qdrant.test"},
            {
                "provider": "ollama",
                "ollama": {
                    "base_url": EMBEDDING_BASE_URL,
                    "model": "nomic-embed-text",
                    "timeout_seconds": 60,
                },
            },
        ),
        local_mode=False,
    )

    class _FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"result": []}

    captured: dict[str, object] = {}

    async def _fake_embed(text: str) -> list[float]:
        captured["embedded_text"] = text
        return [0.11, 0.22, 0.33]

    async def _fake_post(url: str, *, headers: dict[str, str], json: dict[str, object]) -> _FakeResponse:
        captured["url"] = url
        captured["json"] = json
        return _FakeResponse()

    adapter._dims["real"] = 3
    adapter._embedding_provider = type("FakeEmbeddingProvider", (), {"embed": staticmethod(_fake_embed)})()
    adapter._client.post = _fake_post  # type: ignore[method-assign]

    await adapter.search("real", "enterprise infrastructure", 2)

    assert captured["embedded_text"] == "enterprise infrastructure"
    assert captured["json"]["vector"] == [0.11, 0.22, 0.33]


@pytest.mark.asyncio
async def test_chroma_adapter_uses_real_embedding_provider_when_configured() -> None:
    adapter = ChromaAdapter(
        provider_config_with_embeddings(
            "chroma",
            {"enabled": True, "base_url": "https://chroma.test"},
            {
                "provider": "ollama",
                "ollama": {
                    "base_url": EMBEDDING_BASE_URL,
                    "model": "nomic-embed-text",
                    "timeout_seconds": 60,
                },
            },
        ),
        local_mode=False,
    )

    class _FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"ids": [[]], "documents": [[]], "metadatas": [[]], "distances": [[]]}

    captured: dict[str, object] = {}

    async def _fake_embed(text: str) -> list[float]:
        captured["embedded_text"] = text
        return [0.44, 0.55, 0.66]

    async def _fake_post(url: str, *, headers: dict[str, str], json: dict[str, object]) -> _FakeResponse:
        captured["url"] = url
        captured["json"] = json
        return _FakeResponse()

    async def _fake_collection_id(_name: str) -> str:
        return "cid-1"

    adapter._dims["real"] = 3
    adapter._embedding_provider = type("FakeEmbeddingProvider", (), {"embed": staticmethod(_fake_embed)})()
    adapter._client.post = _fake_post  # type: ignore[method-assign]
    adapter._collection_id = _fake_collection_id  # type: ignore[method-assign]

    await adapter.search("real", "enterprise infrastructure", 2)

    assert captured["embedded_text"] == "enterprise infrastructure"
    assert captured["json"]["query_embeddings"] == [[0.44, 0.55, 0.66]]


def test_runtime_client_build_accepts_embedding_config() -> None:
    client = build_runtime_client(
        {
            "vector_stores": {"default_backend": "chroma", "chroma": {"enabled": True, "local_mode": True}},
            "embeddings": {
                "provider": "ollama",
                "ollama": {"base_url": EMBEDDING_BASE_URL, "model": "nomic-embed-text"},
            },
        }
    )
    assert client is not None
