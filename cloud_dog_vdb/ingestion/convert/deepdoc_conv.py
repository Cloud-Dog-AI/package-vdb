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

from cloud_dog_vdb.ingestion.convert.base import Converter


class DeepDocConverter(Converter):
    """Fallback document extractor that preserves paragraph layout."""

    def convert(self, source: str) -> str:
        """Handle convert."""
        paragraphs = [p.strip() for p in source.split("\n\n") if p.strip()]
        return "\n\n".join(paragraphs)
