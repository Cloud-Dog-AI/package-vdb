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
async def test_pt3_5_provider_latency_ranking_is_sortable(
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, object]],
) -> None:
    report, _ = await run_comparison(
        services=services,
        corpus_entries=corpus_entries,
        report_basename="pt3_5_provider_latency_ranking",
    )
    summary = report.get("summary", {})
    assert isinstance(summary, dict)
    latencies = sorted(
        [
            float(values.get("mean_parse_time_ms", 0.0) or 0.0)
            for values in summary.values()
            if isinstance(values, dict) and int(values.get("ok", 0) or 0) > 0
        ]
    )
    assert len(latencies) >= 1
    assert latencies == sorted(latencies)
