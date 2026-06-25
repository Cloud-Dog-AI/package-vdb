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

import pytest

from cloud_dog_vdb.ingestion.ocr.planner import decide_ocr


def test_pt1_5_ocr_modes_follow_corpus_scenarios(corpus_entries: list[dict[str, object]]) -> None:
    require_ocr_seen = False
    for entry in corpus_entries:
        expectations = entry.get("expectations", {})
        if not isinstance(expectations, dict):
            expectations = {}
        require_ocr = bool(expectations.get("require_ocr", False))
        mode = str(entry.get("recommended_ocr_mode", "auto"))
        if require_ocr:
            require_ocr_seen = True
        decision = decide_ocr(
            mode=mode,
            text_chars=0 if require_ocr else int(expectations.get("min_text_chars", 500) or 500),
            scanned_ratio=0.95 if require_ocr else 0.05,
            provider_id="llm_ocr",
        )
        if mode == "force":
            assert decision.enabled is True
        elif mode == "disabled":
            assert decision.enabled is False
        else:
            assert decision.mode == "auto"
            if require_ocr:
                assert decision.enabled is True

    if not require_ocr_seen:
        pytest.skip("Selected corpus slice has no OCR-required entries")
