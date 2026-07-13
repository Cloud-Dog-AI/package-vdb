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

import io

import openpyxl

from cloud_dog_vdb.spreadsheet import extract_workbook, testing
from cloud_dog_vdb.spreadsheet.manifest import diff_manifests


def _resave_with_different_bytes(data: bytes) -> bytes:
    """Re-save a workbook so the bytes differ but the cell content does not."""
    wb = openpyxl.load_workbook(io.BytesIO(data))
    wb.properties.creator = "different-author"  # changes file bytes, not content
    buffer = io.BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def test_unchanged_content_with_different_bytes_is_fully_unchanged():
    # Re-saving a workbook produces different bytes (different file hash) but the
    # same content: object keys + hashes must be stable so refresh detects no
    # change and never deletes/re-upserts (sections 5.14, 5.18).
    data1 = testing.build_simple_xlsx()
    data2 = _resave_with_different_bytes(data1)
    assert data1 != data2

    first = extract_workbook(data1, file_name="s.xlsx", source_uri="file:///s.xlsx")
    second = extract_workbook(data2, file_name="s.xlsx", source_uri="file:///s.xlsx")
    assert first.workbook.file_hash != second.workbook.file_hash
    assert [e.object_key for e in first.manifest] == [e.object_key for e in second.manifest]
    # object keys must not embed the volatile file hash
    assert all(first.workbook.file_hash not in e.object_key for e in first.manifest)

    decisions = diff_manifests(first.manifest, second.manifest)
    assert decisions, "manifest should not be empty"
    assert all(d.refresh_action == "unchanged" for d in decisions)


def test_content_change_marks_upsert():
    v1 = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", source_uri="file:///s.xlsx")
    v2 = extract_workbook(
        testing.build_multisheet_formal_tables_xlsx(), file_name="s.xlsx", source_uri="file:///s.xlsx"
    )
    decisions = {d.object_key: d.refresh_action for d in diff_manifests(v1.manifest, v2.manifest)}
    actions = set(decisions.values())
    # objects removed from v1 are deleted; new objects in v2 are upserted
    assert "delete" in actions
    assert "upsert" in actions


def test_delete_stale_objects_on_shrink():
    big = extract_workbook(testing.build_multisheet_formal_tables_xlsx(), file_name="x.xlsx", source_uri="u")
    small = extract_workbook(testing.build_simple_xlsx(), file_name="x.xlsx", source_uri="u")
    deletes = [d for d in diff_manifests(big.manifest, small.manifest) if d.refresh_action == "delete"]
    assert deletes
    # every delete decision carries the stale object's previous hash for audit
    assert all(d.previous_object_hash for d in deletes)
