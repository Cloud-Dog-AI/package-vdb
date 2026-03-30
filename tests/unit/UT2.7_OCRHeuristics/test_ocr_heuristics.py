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


def _should_use_ocr(text_chars: int, scanned_ratio: float, min_chars: int, min_scanned_ratio: float) -> bool:
    return text_chars < min_chars or scanned_ratio >= min_scanned_ratio


def test_ut2_7_ocr_auto_trigger_heuristic() -> None:
    assert _should_use_ocr(text_chars=50, scanned_ratio=0.9, min_chars=200, min_scanned_ratio=0.5)
    assert not _should_use_ocr(text_chars=5000, scanned_ratio=0.0, min_chars=200, min_scanned_ratio=0.5)
