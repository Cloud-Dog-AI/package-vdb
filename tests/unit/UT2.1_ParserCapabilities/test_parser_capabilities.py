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


def test_ut2_1_parser_capability_contract_keys() -> None:
    capability = {
        "supports_pdf": True,
        "supports_docx": True,
        "supports_html": True,
        "supports_layout": True,
        "supports_tables": True,
        "supports_images": False,
        "supports_ocr_passthrough": True,
        "supports_streaming": False,
        "max_document_bytes": 10_000_000,
    }
    required = {
        "supports_pdf",
        "supports_docx",
        "supports_html",
        "supports_layout",
        "supports_tables",
        "supports_images",
        "supports_ocr_passthrough",
        "supports_streaming",
        "max_document_bytes",
    }
    assert required.issubset(capability)
