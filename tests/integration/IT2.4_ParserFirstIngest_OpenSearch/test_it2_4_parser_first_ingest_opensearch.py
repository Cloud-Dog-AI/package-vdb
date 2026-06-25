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

from tests.integration._parser_ingest_helpers import run_parser_first_roundtrip


@pytest.mark.asyncio
async def test_it2_4_parser_first_ingest_opensearch(
    opensearch_ready: dict,
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, str]],
) -> None:
    await run_parser_first_roundtrip(
        provider_id="opensearch",
        backend_cfg=opensearch_ready,
        services=services,
        corpus_entries=corpus_entries,
    )
