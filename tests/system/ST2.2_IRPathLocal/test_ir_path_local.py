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

import asyncio
from pathlib import Path

import pytest

from cloud_dog_vdb.ingestion.parse.providers.mineru import MineruParserProvider


def _pick_source(corpus_entries: list[dict[str, object]]) -> dict[str, object]:
    preferred_ids = ("item_cod_form", "z83_example", "sample_rural_completed_form", "fw9_form", "examples_pdf")
    for preferred_id in preferred_ids:
        for entry in corpus_entries:
            if str(entry.get("id", "")) == preferred_id:
                return entry
    for entry in corpus_entries:
        if str(entry.get("file", "")).lower().endswith(".pdf"):
            return entry
    return corpus_entries[0]


@pytest.mark.asyncio
async def test_st2_2_ir_path_local_mineru(
    corpus_entries: list[dict[str, object]],
    services: dict[str, dict[str, object]],
) -> None:
    mineru_cfg = services["mineru"]
    if not mineru_cfg.get("enabled"):
        pytest.fail("MINERU_ENABLED must be true for ST2 IR path tests", pytrace=False)
    base_url = str(mineru_cfg.get("base_url", "")).strip()
    if not base_url:
        pytest.fail("MINERU_BASE_URL missing for ST2 IR path tests", pytrace=False)

    package_root = Path(__file__).resolve().parents[3]
    source = _pick_source(corpus_entries)
    source_path = package_root / "test-data" / str(source["file"])
    provider = MineruParserProvider(
        base_url=base_url,
        api_key=str(mineru_cfg.get("api_key", "")),
        timeout_seconds=float(mineru_cfg.get("timeout_seconds", 120.0) or 120.0),
        request_retries=1,
    )
    last_error: Exception | None = None
    ir = None
    for attempt in range(3):
        try:
            ir = await provider.parse_bytes(
                source_path.read_bytes(),
                filename=source_path.name,
                source_uri=f"file://{source_path.name}",
                mime_type="application/pdf",
                options={
                    "parse_backend": "pipeline",
                    "parse_method": "auto",
                },
            )
            break
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                await asyncio.sleep(float((attempt + 1) * 2))
    if ir is None:
        raise last_error if last_error is not None else RuntimeError("MinerU parse failed without an error")

    assert ir.provider_id == "mineru"
    assert ir.provider_version
    assert len(ir.text_blocks) >= 1
