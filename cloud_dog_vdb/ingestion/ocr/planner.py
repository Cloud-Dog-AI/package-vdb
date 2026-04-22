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

from cloud_dog_vdb.ingestion.ocr.base import OCRDecision
from cloud_dog_vdb.ingestion.ocr.heuristics import should_use_ocr_auto


def decide_ocr(
    *,
    mode: str,
    text_chars: int,
    scanned_ratio: float,
    provider_id: str = "",
    min_chars: int = 200,
    min_scanned_ratio: float = 0.5,
) -> OCRDecision:
    """Handle decide ocr."""
    normalised = str(mode or "disabled").strip().lower()
    if normalised == "disabled":
        return OCRDecision(enabled=False, mode=normalised, reason="mode_disabled", provider_id=provider_id)
    if normalised == "force":
        return OCRDecision(enabled=True, mode=normalised, reason="mode_force", provider_id=provider_id)
    enabled = should_use_ocr_auto(
        text_chars=text_chars,
        scanned_ratio=scanned_ratio,
        min_chars=min_chars,
        min_scanned_ratio=min_scanned_ratio,
    )
    reason = "auto_triggered" if enabled else "auto_not_required"
    return OCRDecision(enabled=enabled, mode="auto", reason=reason, provider_id=provider_id)
