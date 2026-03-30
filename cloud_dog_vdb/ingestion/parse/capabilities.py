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

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParserCapabilities:
    """Represent parser capabilities."""
    supports_pdf: bool = True
    supports_docx: bool = False
    supports_html: bool = False
    supports_layout: bool = False
    supports_tables: bool = False
    supports_images: bool = False
    supports_ocr_passthrough: bool = False
    supports_streaming: bool = False
    max_document_bytes: int = 64 * 1024 * 1024
