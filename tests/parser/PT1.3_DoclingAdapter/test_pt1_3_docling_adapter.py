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

from tests.parser._provider_matrix import evaluate_provider


@pytest.mark.asyncio
async def test_pt1_3_docling_corpus_quality_matrix(
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, object]],
) -> None:
    docling = services["docling"]
    if not docling.get("enabled"):
        pytest.skip("Docling parser tests disabled; set DOCLING_ENABLED=true to enforce")

    summary = await evaluate_provider(provider_id="docling", services=services, corpus_entries=corpus_entries)
    soft_fail = str(summary.get("soft_fail_reason", "")).strip()
    if soft_fail == "provider_busy":
        pytest.xfail(f"{summary.get('provider_id')} busy: {summary.get('soft_fail_detail', '')}")

    expected_categories = sorted({str(entry.get("category", "")) for entry in corpus_entries})
    assert summary["total_docs"] == len(corpus_entries)
    assert summary["categories_covered"] == expected_categories
    assert float(summary["success_ratio"]) >= 0.90
    assert float(summary["quality_invariant_pass_rate"]) >= 0.70
