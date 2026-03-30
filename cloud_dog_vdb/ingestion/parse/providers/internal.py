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

from cloud_dog_vdb.ingestion.parse.base import ParserProvider
from cloud_dog_vdb.ingestion.parse.capabilities import ParserCapabilities
from cloud_dog_vdb.ingestion.parse.ir import DocumentIR, TextBlock


def _decode_best_effort(document: bytes) -> str:
    for encoding in ("utf-8", "utf-16", "latin-1"):
        try:
            return document.decode(encoding)
        except UnicodeDecodeError:
            continue
    return document.decode("utf-8", errors="replace")


def _to_text_blocks(text: str) -> list[TextBlock]:
    paragraphs = [part.strip() for part in text.replace("\r\n", "\n").split("\n\n") if part.strip()]
    if not paragraphs:
        stripped = text.strip()
        return [TextBlock(text=stripped)] if stripped else []
    return [TextBlock(text=paragraph) for paragraph in paragraphs]


class InternalParserProvider(ParserProvider):
    """Represent internal parser provider."""
    provider_id = "internal"
    provider_version = "1.0"

    @property
    def capabilities(self) -> ParserCapabilities:
        """Handle capabilities."""
        return ParserCapabilities(
            supports_pdf=False,
            supports_docx=False,
            supports_html=True,
            supports_layout=False,
            supports_tables=False,
            supports_images=False,
            supports_ocr_passthrough=False,
            supports_streaming=False,
        )

    async def health_check(self) -> bool:
        """Handle health check."""
        return True

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
        _ = (filename, mime_type, options)
        text = _decode_best_effort(document)
        return DocumentIR(
            source_uri=source_uri,
            provider_id=self.provider_id,
            provider_version=self.provider_version,
            text_blocks=_to_text_blocks(text),
            quality={"confidence": 1.0},
        )
