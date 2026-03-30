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

from cloud_dog_vdb.domain.errors import BackendUnavailableError, InvalidRequestError
from cloud_dog_vdb.ingestion.parse.base import ParserProvider
from cloud_dog_vdb.ingestion.parse.capabilities import ParserCapabilities
from cloud_dog_vdb.ingestion.parse.ir import DocumentIR
from cloud_dog_vdb.ingestion.parse.providers.internal import _decode_best_effort, _to_text_blocks


class DeepDocParserProvider(ParserProvider):
    """Represent deep doc parser provider."""
    provider_id = "deepdoc"
    provider_version = "command-1"

    def __init__(self, *, command: list[str] | None = None, timeout_seconds: float = 120.0) -> None:
        self.command = command or ["deepdoc"]
        self.timeout_seconds = timeout_seconds

    @property
    def capabilities(self) -> ParserCapabilities:
        """Handle capabilities."""
        return ParserCapabilities(
            supports_pdf=True,
            supports_docx=True,
            supports_html=True,
            supports_layout=True,
            supports_tables=True,
            supports_images=False,
            supports_ocr_passthrough=False,
        )

    async def health_check(self) -> bool:
        """Handle health check."""
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
        _ = mime_type
        args = list((options or {}).get("command", self.command))
        if not args:
            raise BackendUnavailableError("deepdoc command is not configured")
        with TemporaryDirectory(prefix="cdvdb-deepdoc-") as tmp:
            source_path = Path(tmp) / filename
            source_path.write_bytes(document)
            proc = await asyncio.create_subprocess_exec(
                *args,
                str(source_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self.timeout_seconds)
            if proc.returncode != 0:
                raise InvalidRequestError(f"deepdoc command failed: {stderr.decode('utf-8', errors='replace')[:200]}")
            text = stdout.decode("utf-8", errors="replace").strip()
        if not text:
            text = _decode_best_effort(document).strip()
        return DocumentIR(
            source_uri=source_uri,
            provider_id=self.provider_id,
            provider_version=self.provider_version,
            text_blocks=_to_text_blocks(text),
            quality={"confidence": 0.8},
        )
