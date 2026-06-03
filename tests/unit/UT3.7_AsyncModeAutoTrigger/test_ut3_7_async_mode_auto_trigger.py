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

from typing import Any

import pytest

from cloud_dog_vdb.ingestion.parse.providers import marker_mcp as marker_module
from cloud_dog_vdb.ingestion.parse.providers.marker_mcp import MarkerMcpParserProvider


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict[str, Any],
        text: str = "",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = headers or {}

    def json(self) -> dict[str, Any]:
        return self._payload


_MCP_INIT_RESPONSE: dict[str, Any] = {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "serverInfo": {"name": "marker-mcp-fake", "version": "0.1.0"},
    },
}


def _mcp_tool_result(payload: dict[str, Any]) -> dict[str, Any]:
    import json

    return {
        "jsonrpc": "2.0",
        "id": 2,
        "result": {"content": [{"type": "text", "text": json.dumps(payload)}]},
    }


class _FakeMarkerClient:
    def __init__(self) -> None:
        self.post_calls: list[str] = []

    async def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.post_calls.append(url)
        body = kwargs.get("json", {})
        method = body.get("method", "")
        if method == "initialize":
            return _FakeResponse(200, _MCP_INIT_RESPONSE, headers={"mcp-session-id": "fake"})
        if method == "notifications/initialized":
            return _FakeResponse(202, {})
        if method == "tools/call":
            return _FakeResponse(
                200,
                _mcp_tool_result(
                    {
                        "success": True,
                        "output": "# Async fallback output",
                        "metadata": {"table_of_contents": []},
                    }
                ),
            )
        return _FakeResponse(200, {})

    async def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        _ = (url, kwargs)
        return _FakeResponse(200, {"status": "healthy"})


@pytest.mark.asyncio
async def test_ut3_7_marker_auto_trigger_enters_async_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    state: dict[str, Any] = {"used": False}

    class _FakeRunner:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            _ = (args, kwargs)

        async def run(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            state["used"] = True
            sync_fallback = kwargs["sync_fallback"]
            return await sync_fallback()

    monkeypatch.setattr(marker_module, "AsyncParseRunner", _FakeRunner)

    provider = MarkerMcpParserProvider(base_url="https://marker.example.test", timeout_seconds=30.0)
    provider._client = _FakeMarkerClient()  # type: ignore[attr-defined]

    ir = await provider.parse_bytes(
        b"x" * (4 * 1024 * 1024),
        filename="large.pdf",
        source_uri="file://large.pdf",
        mime_type="application/pdf",
        options={"async_threshold_seconds": 5.0, "estimated_bytes_per_second": 250_000.0},
    )

    assert state["used"] is True
    assert ir.metadata.get("execution_mode") == "sync_fallback"
