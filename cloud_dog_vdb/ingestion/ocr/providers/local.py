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
from cloud_dog_vdb.ingestion.ocr.base import OCRProvider


class LocalOCRProvider(OCRProvider):
    """Represent local o c r provider."""

    provider_id = "local"

    def __init__(self, *, command: list[str] | None = None, timeout_seconds: float = 120.0) -> None:
        self.command = command or ["tesseract"]
        self.timeout_seconds = timeout_seconds

    async def health_check(self) -> bool:
        """Handle health check."""
        try:
            proc = await asyncio.create_subprocess_exec(
                *self.command,
                "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=self.timeout_seconds)
            return proc.returncode == 0
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
        _ = (mime_type, options)
        if not self.command:
            raise BackendUnavailableError("local OCR command is not configured")
        with TemporaryDirectory(prefix="cdvdb-ocr-") as tmp:
            source_path = Path(tmp) / filename
            output_stem = Path(tmp) / "ocr-out"
            source_path.write_bytes(document)
            proc = await asyncio.create_subprocess_exec(
                *self.command,
                str(source_path),
                str(output_stem),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=self.timeout_seconds)
            if proc.returncode != 0:
                raise InvalidRequestError(f"local OCR failed: {stderr.decode('utf-8', errors='replace')[:200]}")
            out_file = output_stem.with_suffix(".txt")
            if not out_file.exists():
                return ""
            return out_file.read_text(encoding="utf-8", errors="replace").strip()
