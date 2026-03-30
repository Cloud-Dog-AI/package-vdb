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

from cloud_dog_vdb.ingestion.chunk.base import Chunker


class RecursiveChunker(Chunker):
    """Represent recursive chunker."""
    def __init__(self, max_chars: int = 500) -> None:
        self._max_chars = max_chars

    def chunk(self, text: str) -> list[str]:
        """Handle chunk."""
        parts = [p.strip() for p in text.split("\n\n") if p.strip()]
        if len(parts) <= 1:
            if len(text) <= self._max_chars:
                return [text]
            return [text[i : i + self._max_chars] for i in range(0, len(text), self._max_chars)]
        return parts
