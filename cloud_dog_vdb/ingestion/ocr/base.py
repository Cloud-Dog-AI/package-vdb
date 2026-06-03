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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class OCRDecision:
    """Represent o c r decision."""

    enabled: bool
    mode: str
    reason: str
    provider_id: str = ""


class OCRProvider(ABC):
    """Represent o c r provider."""

    provider_id: str

    @abstractmethod
    async def health_check(self) -> bool:
        """Handle health check."""
        raise NotImplementedError

    @abstractmethod
    async def extract_text(
        self,
        document: bytes,
        *,
        filename: str,
        mime_type: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> str:
        """Handle extract text."""
        raise NotImplementedError
