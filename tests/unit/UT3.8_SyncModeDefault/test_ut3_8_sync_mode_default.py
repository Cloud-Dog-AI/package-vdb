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

from cloud_dog_vdb.domain.errors import InvalidRequestError
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


def _mcp_tool_result(payload: dict[str, Any]) -> dict[str, Any]:
    import json

    return {
        "jsonrpc": "2.0",
        "id": 2,
        "result": {"content": [{"type": "text", "text": json.dumps(payload)}]},
    }


class _FakeMarkerClient:
    async def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        body = kwargs.get("json", {})
        method = body.get("method", "")
        if method == "initialize":
            return _FakeResponse(
                200,
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {},
                        "serverInfo": {"name": "fake", "version": "0.1.0"},
                    },
                },
                headers={"mcp-session-id": "fake"},
            )
        if method == "notifications/initialized":
            return _FakeResponse(202, {})
        if method == "tools/call":
            return _FakeResponse(200, _mcp_tool_result({"success": True, "output": "# Sync output"}))
        return _FakeResponse(200, {})

    async def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        _ = (url, kwargs)
        return _FakeResponse(200, {"status": "healthy"})


@pytest.mark.asyncio
async def test_ut3_8_marker_sync_mode_remains_default(monkeypatch: pytest.MonkeyPatch) -> None:
    class _ForbiddenRunner:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise AssertionError("Async runner should not be used in default sync mode")

    monkeypatch.setattr(marker_module, "AsyncParseRunner", _ForbiddenRunner)

    provider = MarkerMcpParserProvider(base_url="https://marker.example.test", timeout_seconds=30.0)
    provider._client = _FakeMarkerClient()  # type: ignore[attr-defined]

    ir = await provider.parse_bytes(
        b"small",
        filename="small.pdf",
        source_uri="file://small.pdf",
        mime_type="application/pdf",
    )
    assert ir.metadata.get("execution_mode") == "sync"
    assert ir.full_text().strip() == "# Sync output"


@pytest.mark.asyncio
async def test_ut3_8_marker_rejects_unsupported_text_mime_early() -> None:
    provider = MarkerMcpParserProvider(base_url="https://marker.example.test", timeout_seconds=30.0)
    with pytest.raises(InvalidRequestError, match="unsupported document type"):
        await provider.parse_bytes(
            b"plain text content",
            filename="fallback.txt",
            source_uri="file://fallback.txt",
            mime_type="text/plain",
        )
