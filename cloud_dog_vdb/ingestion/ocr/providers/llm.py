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

import base64
from typing import Any

import httpx

from cloud_dog_vdb.domain.errors import BackendUnavailableError, InvalidRequestError
from cloud_dog_vdb.ingestion.ocr.base import OCRProvider


class LlmOCRProvider(OCRProvider):
    """Represent llm o c r provider."""
    provider_id = "llm_ocr"

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: float = 180.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def health_check(self) -> bool:
        """Handle health check."""
        if not self.base_url:
            return False
        try:
            resp = await self._client.get(f"{self.base_url}/models", headers=self._headers())
            return resp.status_code < 400
        except Exception:
            return False

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def extract_text(
        self,
        document: bytes,
        *,
        filename: str,
        mime_type: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> str:
        """Handle extract text."""
        _ = filename
        if not self.base_url:
            raise BackendUnavailableError("LLM OCR base_url is not configured")
        prompt = str((options or {}).get("prompt", "Extract all readable text from this document image."))
        data_uri = f"data:{mime_type or 'application/octet-stream'};base64,{base64.b64encode(document).decode()}"
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_uri}},
                    ],
                }
            ],
            "temperature": 0,
        }
        resp = await self._client.post(f"{self.base_url}/chat/completions", headers=self._headers(), json=payload)
        if resp.status_code >= 400:
            raise InvalidRequestError(f"LLM OCR request failed: {resp.status_code} {resp.text[:200]}")
        choices = resp.json().get("choices") or []
        if not choices:
            return ""
        message = (choices[0] or {}).get("message") or {}
        return str(message.get("content", "")).strip()
