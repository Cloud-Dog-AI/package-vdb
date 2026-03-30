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

import httpx

from cloud_dog_vdb.embeddings.base import EmbeddingProvider
from cloud_dog_vdb.domain.errors import InvalidRequestError


class OllamaEmbeddingProvider(EmbeddingProvider):
    """Represent ollama embedding provider."""
    def __init__(self, base_url: str, model: str, timeout_seconds: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def embed(self, text: str) -> list[float]:
        """Handle embed."""
        resp = await self._client.post(
            f"{self.base_url}/api/embeddings",
            json={"model": self.model, "prompt": text},
        )
        if resp.status_code >= 400:
            raise InvalidRequestError(f"Ollama embedding failed: {resp.status_code}")
        return [float(x) for x in (resp.json().get("embedding") or [])]


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """Represent open a i embedding provider."""
    def __init__(self, base_url: str, api_key: str, model: str, timeout_seconds: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def embed(self, text: str) -> list[float]:
        """Handle embed."""
        resp = await self._client.post(
            f"{self.base_url}/embeddings",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json={"model": self.model, "input": [text]},
        )
        if resp.status_code >= 400:
            raise InvalidRequestError(f"OpenAI-compatible embedding failed: {resp.status_code}")
        data = resp.json().get("data") or []
        if not data:
            return []
        return [float(x) for x in (data[0].get("embedding") or [])]
