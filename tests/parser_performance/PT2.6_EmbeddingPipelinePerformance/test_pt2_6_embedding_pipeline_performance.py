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

from pathlib import Path

import pytest

from tests.parser._provider_matrix import enabled_parser_ids, evaluate_enabled_providers


@pytest.mark.asyncio
async def test_pt2_6_parser_quality_reports_are_materialised(
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, object]],
) -> None:
    enabled = enabled_parser_ids(services)
    if not enabled:
        pytest.fail("No parser providers are enabled for PT2 report validation", pytrace=False)

    matrix = await evaluate_enabled_providers(services=services, corpus_entries=corpus_entries)
    report_root = Path(__file__).resolve().parents[3] / "tests" / "reports"
    for provider_id in enabled:
        report_path = report_root / f"parser_quality_matrix_{provider_id}.json"
        assert report_path.is_file(), f"missing parser report: {report_path}"
        assert int(matrix[provider_id]["total_docs"]) == len(corpus_entries)
