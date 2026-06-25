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

from tests.parser._comparison_helpers import run_comparison


@pytest.mark.asyncio
async def test_pt3_7_image_extraction_metrics_present(
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, object]],
) -> None:
    report, _ = await run_comparison(
        services=services,
        corpus_entries=corpus_entries,
        report_basename="pt3_7_image_extraction_comparison",
    )
    ok_cases = [case for case in report.get("cases", []) if isinstance(case, dict) and case.get("status") == "ok"]
    assert len(ok_cases) >= 1
    for case in ok_cases:
        assert "image_count" in case
        assert int(case.get("image_count", 0) or 0) >= 0

    marker_cases = [case for case in ok_cases if case.get("provider_id") == "marker_mcp"]
    if marker_cases:
        assert max(int(case.get("image_count", 0) or 0) for case in marker_cases) >= 1
