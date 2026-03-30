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


def should_use_ocr_auto(
    *,
    text_chars: int,
    scanned_ratio: float,
    min_chars: int = 200,
    min_scanned_ratio: float = 0.5,
) -> bool:
    """Handle should use ocr auto."""
    return int(text_chars) < int(min_chars) and float(scanned_ratio) >= float(min_scanned_ratio)


def ocr_cost_within_budget(*, estimated_cost: float, max_cost_per_document: float) -> bool:
    """Handle ocr cost within budget."""
    return float(estimated_cost) <= float(max_cost_per_document)
