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

import os

import pytest

from tests.parser._provider_matrix import enabled_parser_ids, evaluate_enabled_providers


def _quality_floor(corpus_manifest: dict[str, object]) -> float:
    env = os.environ.get("PARSER_MIN_QUALITY_INVARIANT_PASS_RATE", "").strip()
    if env:
        try:
            return float(env)
        except ValueError:
            pass
    baselines = corpus_manifest.get("performance_baselines", {})
    if isinstance(baselines, dict):
        thresholds = baselines.get("thresholds", {})
        if isinstance(thresholds, dict):
            value = thresholds.get("minimum_quality_invariant_pass_rate")
            if isinstance(value, (int, float)):
                return float(value)
    return 0.95


@pytest.mark.asyncio
async def test_pt2_4_quality_invariant_pass_rate_meets_floor(
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, object]],
    corpus_manifest: dict[str, object],
) -> None:
    enabled = enabled_parser_ids(services)
    if not enabled:
        pytest.fail("No parser providers are enabled for PT2 quality benchmarks", pytrace=False)

    matrix = await evaluate_enabled_providers(services=services, corpus_entries=corpus_entries)
    floor = _quality_floor(corpus_manifest)
    for provider_id in enabled:
        observed = float(matrix[provider_id]["quality_invariant_pass_rate"])
        assert observed >= floor, (
            f"{provider_id} quality_invariant_pass_rate below floor: observed={observed:.3f}, floor={floor:.3f}"
        )
