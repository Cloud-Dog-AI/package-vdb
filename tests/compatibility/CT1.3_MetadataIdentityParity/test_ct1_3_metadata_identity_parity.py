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

from cloud_dog_vdb.metadata.identity import compute_doc_id, compute_record_id


def test_ct1_3_identity_functions_are_stable() -> None:
    doc_id = compute_doc_id("file://doc", "a" * 64)
    record_id = compute_record_id(doc_id, 0)
    assert len(doc_id) == 64
    assert len(record_id) == 64
