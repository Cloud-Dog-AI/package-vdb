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

from cloud_dog_vdb.domain.errors import ParserTimeoutError
from cloud_dog_vdb.ingestion.parse import async_runner as async_runner_module
from cloud_dog_vdb.ingestion.parse.async_runner import AsyncParseRunner


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any], text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> dict[str, Any]:
        return self._payload


class _CancellationClient:
    def __init__(self) -> None:
        self.cancel_calls: list[str] = []

    async def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        _ = kwargs
        if "/cancel/" in url:
            self.cancel_calls.append(url)
            return _FakeResponse(200, {"ok": True})
        return _FakeResponse(200, {"job_id": "job-cancel"})

    async def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        _ = (url, kwargs)
        return _FakeResponse(200, {"status": "queued"})


@pytest.mark.asyncio
async def test_ut3_6_async_runner_sends_cancel_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    values = [0.0, 2.5, 2.5]
    state = {"index": 0}

    def _fake_monotonic() -> float:
        index = state["index"]
        state["index"] = index + 1
        if index >= len(values):
            return values[-1]
        return values[index]

    monkeypatch.setattr(async_runner_module.time, "monotonic", _fake_monotonic)

    client = _CancellationClient()
    runner = AsyncParseRunner(client, poll_interval=0.01, max_wait=1.0, timeout=5.0)
    with pytest.raises(ParserTimeoutError):
        await runner.run(
            "https://marker.example.test/submit",
            b"%PDF-1.7\n",
            "sample.pdf",
            status_url="https://marker.example.test/status/{job_id}",
            result_url="https://marker.example.test/result/{job_id}",
            cancel_url="https://marker.example.test/cancel/{job_id}",
        )

    assert client.cancel_calls == ["https://marker.example.test/cancel/job-cancel"]
