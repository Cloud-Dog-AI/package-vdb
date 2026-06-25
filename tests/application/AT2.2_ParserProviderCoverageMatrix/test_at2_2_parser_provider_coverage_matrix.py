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

from tests.integration._parser_ingest_helpers import run_parser_first_roundtrip


REQUIRED_PARSER_PROVIDERS = ("mineru", "deepdoc", "docling", "transformers")


def _is_true(raw: str) -> bool:
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


@pytest.mark.asyncio
async def test_at2_2_parser_provider_coverage_matrix(
    chroma_ready: dict,
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, str]],
) -> None:
    if _is_true(os.environ.get("REQUIRE_ALL_PDF_PARSERS", "")):
        missing = [provider_id for provider_id in REQUIRED_PARSER_PROVIDERS if not services[provider_id].get("enabled")]
        if missing:
            pytest.fail(
                "REQUIRE_ALL_PDF_PARSERS is set but providers are disabled: " + ", ".join(sorted(missing)),
                pytrace=False,
            )

    enabled = [provider_id for provider_id in REQUIRED_PARSER_PROVIDERS if services[provider_id].get("enabled")]
    if not enabled:
        pytest.fail("No parser providers enabled for AT2.2 parser coverage matrix", pytrace=False)

    for parser_provider in enabled:
        await run_parser_first_roundtrip(
            provider_id="chroma",
            backend_cfg=chroma_ready,
            services=services,
            corpus_entries=corpus_entries,
            parser_chain=[parser_provider, "internal"],
            parser_options_override={
                "mineru": {
                    "parse_backend": "pipeline",
                    "parse_method": "auto",
                    "page_fallback_target_chars": 600,
                    "page_fallback_max_pages": 3,
                },
            },
            required_parser_provider=parser_provider,
        )
