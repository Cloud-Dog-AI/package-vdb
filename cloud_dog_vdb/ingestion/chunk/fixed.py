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


class FixedChunker(Chunker):
    """Represent fixed chunker."""
    def __init__(self, size: int = 100) -> None:
        self.size = size

    def chunk(self, text: str) -> list[str]:
        """Handle chunk."""
        return [text[i : i + self.size] for i in range(0, len(text), self.size)] or [""]
