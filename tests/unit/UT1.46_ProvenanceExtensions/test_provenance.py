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

from cloud_dog_vdb.metadata.provenance import build_provenance_patch, merge_provenance


def test_provenance_patch_builds_expected_fields() -> None:
    patch = build_provenance_patch(parser_provider="mineru", parser_version="1.2.3", page=4, chunk_kind="table")
    assert patch == {
        "parser_provider": "mineru",
        "parser_version": "1.2.3",
        "page": 4,
        "chunk_kind": "table",
    }


def test_provenance_merge_is_additive_and_preserves_existing_values() -> None:
    merged = merge_provenance(
        {"parser_provider": "deepdoc", "ocr_provider": "existing-ocr", "page_number": 7},
        {
            "parser_provider": "mineru",
            "ocr_engine": "tesseract",
            "ocr_confidence": 0.88,
            "page": 9,
            "section": "appendix",
        },
    )
    assert merged["parser_provider"] == "deepdoc"
    assert merged["ocr_engine"] == "existing-ocr"
    assert merged["ocr_provider"] == "existing-ocr"
    assert merged["page"] == 7
    assert merged["page_number"] == 7
    assert merged["ocr_confidence"] == 0.88
    assert merged["section"] == "appendix"
    assert merged["section_path"] == "appendix"
