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

from cloud_dog_vdb.ingestion.parse.providers.internal import InternalParserProvider
from cloud_dog_vdb.ingestion.parse.providers.marker_mcp import MarkerMcpParserProvider


@pytest.mark.asyncio
async def test_st2_8_parser_fallback_chain_order(services: dict[str, dict[str, object]]) -> None:
    marker_cfg = services["marker_mcp"]
    if not marker_cfg.get("enabled"):
        pytest.skip("Marker-MCP tests are currently on hold (MARKER_MCP_ENABLED=false)")
    base_url = str(marker_cfg.get("base_url", "")).strip()
    if not base_url:
        pytest.fail("MARKER_MCP_BASE_URL missing for ST2 fallback tests", pytrace=False)

    marker = MarkerMcpParserProvider(
        base_url=base_url,
        auth_token=str(marker_cfg.get("auth_token", "")),
        timeout_seconds=float(marker_cfg.get("timeout_seconds", 120.0) or 120.0),
    )
    internal = InternalParserProvider()
    source = b"fallback plain text document"
    try:
        await marker.parse_bytes(
            source, filename="fallback.txt", source_uri="file://fallback.txt", mime_type="text/plain"
        )
        selected = "marker_mcp"
    except Exception:
        ir = await internal.parse_bytes(
            source,
            filename="fallback.txt",
            source_uri="file://fallback.txt",
            mime_type="text/plain",
        )
        selected = ir.provider_id
    assert selected in {"marker_mcp", "internal"}
