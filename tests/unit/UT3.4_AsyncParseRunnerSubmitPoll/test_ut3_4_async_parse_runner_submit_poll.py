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

from cloud_dog_vdb.ingestion.parse.async_runner import AsyncParseRunner


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any], text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> dict[str, Any]:
        return self._payload


class _LifecycleClient:
    def __init__(self) -> None:
        self.status_calls = 0
        self.post_calls: list[str] = []
        self.get_calls: list[str] = []

    async def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        _ = kwargs
        self.post_calls.append(url)
        return _FakeResponse(200, {"job_id": "job-123"})

    async def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        _ = kwargs
        self.get_calls.append(url)
        if url.endswith("/status/job-123"):
            self.status_calls += 1
            if self.status_calls == 1:
                return _FakeResponse(200, {"status": "running"})
            return _FakeResponse(200, {"status": "complete", "success": True})
        if url.endswith("/result/job-123"):
            return _FakeResponse(200, {"success": True, "output": "# parsed"})
        return _FakeResponse(404, {"error": "not_found"}, text="not found")


@pytest.mark.asyncio
async def test_ut3_4_async_submit_poll_retrieve_lifecycle() -> None:
    client = _LifecycleClient()
    events: list[dict[str, Any]] = []
    runner = AsyncParseRunner(client, poll_interval=0.01, max_wait=5.0, timeout=5.0, progress_callback=events.append)

    payload = await runner.run(
        "https://marker.example.test/submit",
        b"%PDF-1.7\n",
        "sample.pdf",
        headers={"X-Test": "1"},
        status_url="https://marker.example.test/status/{job_id}",
        result_url="https://marker.example.test/result/{job_id}",
    )

    assert payload["output"] == "# parsed"
    assert client.post_calls == ["https://marker.example.test/submit"]
    assert "https://marker.example.test/status/job-123" in client.get_calls
    assert "https://marker.example.test/result/job-123" in client.get_calls
    assert any(event.get("event") == "poll" for event in events)
