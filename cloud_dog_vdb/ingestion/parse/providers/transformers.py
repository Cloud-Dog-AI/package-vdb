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

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import httpx

from cloud_dog_vdb.domain.errors import BackendUnavailableError, InvalidRequestError
from cloud_dog_vdb.ingestion.parse.base import ParserProvider
from cloud_dog_vdb.ingestion.parse.capabilities import ParserCapabilities
from cloud_dog_vdb.ingestion.parse.ir import DocumentIR
from cloud_dog_vdb.ingestion.parse.providers.internal import _decode_best_effort, _to_text_blocks


def _coerce_text(payload: dict[str, Any]) -> str:
    for key in ("text", "markdown", "content"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    result = payload.get("result")
    if isinstance(result, dict):
        for key in ("text", "markdown", "content"):
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                return value
    return ""


class TransformersParserProvider(ParserProvider):
    """Represent transformers parser provider."""

    provider_id = "transformers"
    provider_version = "adapter-0.1.0"

    def __init__(
        self,
        *,
        base_url: str = "",
        api_key: str = "",
        endpoint_path: str = "/parse",
        command: list[str] | None = None,
        timeout_seconds: float = 120.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.endpoint_path = endpoint_path if endpoint_path.startswith("/") else f"/{endpoint_path}"
        self.command = command or []
        self.timeout_seconds = timeout_seconds
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    @property
    def capabilities(self) -> ParserCapabilities:
        """Handle capabilities."""
        return ParserCapabilities(
            supports_pdf=True,
            supports_docx=True,
            supports_html=True,
            supports_layout=True,
            supports_tables=True,
            supports_images=True,
            supports_ocr_passthrough=True,
            supports_streaming=False,
            max_document_bytes=256 * 1024 * 1024,
        )

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def health_check(self) -> bool:
        """Handle health check."""
        if self.base_url:
            try:
                for path in ("/openapi.json", "/health", "/healthz"):
                    resp = await self._client.get(f"{self.base_url}{path}", headers=self._headers())
                    if resp.status_code == 200:
                        return True
                return False
            except Exception:
                return False
        if self.command:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *self.command,
                    "--help",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await asyncio.wait_for(proc.communicate(), timeout=self.timeout_seconds)
                return proc.returncode == 0
            except Exception:
                return False
        return False

    async def parse_bytes(
        self,
        document: bytes,
        *,
        filename: str,
        source_uri: str,
        mime_type: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> DocumentIR:
        """Parse bytes."""
        opts = options or {}

        if self.base_url:
            endpoint_path = str(opts.get("endpoint_path", self.endpoint_path))
            endpoint_path = endpoint_path if endpoint_path.startswith("/") else f"/{endpoint_path}"
            files = {"file": (filename, document, mime_type or "application/octet-stream")}
            resp = await self._client.post(
                f"{self.base_url}{endpoint_path}",
                headers=self._headers(),
                files=files,
            )
            if resp.status_code >= 400:
                raise InvalidRequestError(f"transformers parse failed: {resp.status_code} {resp.text[:200]}")
            payload = resp.json()
            text = _coerce_text(payload).strip()
            if not text:
                text = _decode_best_effort(document).strip()
            return DocumentIR(
                source_uri=source_uri,
                provider_id=self.provider_id,
                provider_version=self.provider_version,
                text_blocks=_to_text_blocks(text),
                quality={"confidence": 0.85},
                metadata={"mode": "http"},
            )

        command = list(opts.get("command", self.command))
        if not command:
            raise BackendUnavailableError("transformers parser requires base_url or command")
        with TemporaryDirectory(prefix="cdvdb-transformers-") as tmp:
            source_path = Path(tmp) / filename
            source_path.write_bytes(document)
            proc = await asyncio.create_subprocess_exec(
                *command,
                str(source_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self.timeout_seconds)
            if proc.returncode != 0:
                raise InvalidRequestError(
                    f"transformers command failed: {stderr.decode('utf-8', errors='replace')[:200]}"
                )
            text = stdout.decode("utf-8", errors="replace").strip()
        if not text:
            text = _decode_best_effort(document).strip()
        return DocumentIR(
            source_uri=source_uri,
            provider_id=self.provider_id,
            provider_version=self.provider_version,
            text_blocks=_to_text_blocks(text),
            quality={"confidence": 0.8},
            metadata={"mode": "command"},
        )
