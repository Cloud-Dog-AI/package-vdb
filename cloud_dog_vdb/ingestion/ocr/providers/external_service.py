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

from typing import Any

import httpx

from cloud_dog_vdb.domain.errors import BackendUnavailableError, InvalidRequestError
from cloud_dog_vdb.ingestion.ocr.base import OCRProvider


class ExternalServiceOCRProvider(OCRProvider):
    """Represent external service o c r provider."""
    provider_id = "external_service"

    def __init__(self, *, base_url: str, api_key: str = "", timeout_seconds: float = 120.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def health_check(self) -> bool:
        """Handle health check."""
        if not self.base_url:
            return False
        try:
            resp = await self._client.get(f"{self.base_url}/health", headers=self._headers())
            return resp.status_code < 400
        except Exception:
            return False

    async def extract_text(
        self,
        document: bytes,
        *,
        filename: str,
        mime_type: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> str:
        """Handle extract text."""
        _ = options
        if not self.base_url:
            raise BackendUnavailableError("external OCR service base_url is not configured")
        files = {"file": (filename, document, mime_type or "application/octet-stream")}
        resp = await self._client.post(f"{self.base_url}/ocr", headers=self._headers(), files=files)
        if resp.status_code >= 400:
            raise InvalidRequestError(f"external OCR request failed: {resp.status_code} {resp.text[:200]}")
        body = resp.json()
        text = body.get("text", "")
        return str(text).strip()
