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
async def test_pt3_1_small_corpus_comparison_outputs_report(
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, object]],
) -> None:
    report, paths = await run_comparison(
        services=services,
        corpus_entries=corpus_entries,
        report_basename="pt3_1_corpus_small_comparison",
    )
    assert paths["json"].is_file()
    assert paths["markdown"].is_file()
    assert len(report.get("cases", [])) >= len(corpus_entries)
    assert "internal" in set(report.get("providers", []))
