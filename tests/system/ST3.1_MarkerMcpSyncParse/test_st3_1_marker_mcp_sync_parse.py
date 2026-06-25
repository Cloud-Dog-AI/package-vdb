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

from cloud_dog_vdb.ingestion.parse.providers.marker_mcp import MarkerMcpParserProvider


def _entry_by_id(corpus_entries: list[dict[str, object]], entry_id: str) -> dict[str, object]:
    for entry in corpus_entries:
        if str(entry.get("id", "")) == entry_id:
            return entry
    pytest.fail(f"Required corpus entry '{entry_id}' is missing from the active corpus slice", pytrace=False)


@pytest.mark.asyncio
async def test_st3_1_marker_mcp_sync_parse_live(
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, object]],
) -> None:
    marker_cfg = services["marker_mcp"]
    if not marker_cfg.get("enabled"):
        pytest.fail("MARKER_MCP_ENABLED must be true for ST3 Marker tests", pytrace=False)
    base_url = str(marker_cfg.get("base_url", "")).strip()
    if not base_url:
        pytest.fail("MARKER_MCP_BASE_URL missing for ST3 Marker tests", pytrace=False)

    package_root = Path(__file__).resolve().parents[3]
    entry = _entry_by_id(corpus_entries, "item_cod_form")
    source_path = package_root / "test-data" / str(entry.get("file", ""))
    if not source_path.is_file():
        pytest.fail(f"Missing corpus file: {source_path}", pytrace=False)

    provider = MarkerMcpParserProvider(
        base_url=base_url,
        auth_token=str(marker_cfg.get("auth_token", "")),
        timeout_seconds=float(marker_cfg.get("timeout_seconds", 300.0) or 300.0),
    )
    ir = await provider.parse_bytes(
        source_path.read_bytes(),
        filename=source_path.name,
        source_uri=f"file://{source_path.name}",
        mime_type="application/pdf",
        options={"async_mode": False, "output_format": "markdown", "paginate_output": True, "page_range": 0},
    )

    assert ir.provider_id == "marker_mcp"
    assert ir.metadata.get("execution_mode") == "sync"
    assert ir.full_text().strip() != ""
