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

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class TextBlock:
    """Represent text block."""

    text: str
    kind: str = "paragraph"
    page: int | None = None
    section_path: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TableBlock:
    """Represent table block."""

    headers: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    page: int | None = None
    locator: str = ""


@dataclass(frozen=True, slots=True)
class DocumentIR:
    """Represent document i r."""

    source_uri: str
    provider_id: str
    provider_version: str
    text_blocks: list[TextBlock] = field(default_factory=list)
    table_blocks: list[TableBlock] = field(default_factory=list)
    artefacts: list[str] = field(default_factory=list)
    artefact_refs: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    quality: dict[str, float] = field(default_factory=dict)

    def full_text(self) -> str:
        """Handle full text."""
        return "\n\n".join(block.text.strip() for block in self.text_blocks if block.text.strip())
