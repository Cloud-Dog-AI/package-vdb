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

import httpx
import pytest

from cloud_dog_vdb.ingestion.parse.providers import mineru as mineru_module
from cloud_dog_vdb.ingestion.parse.providers.mineru import MineruParserProvider


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any] | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeMineruClient:
    def __init__(self, responder) -> None:
        self._responder = responder
        self.calls: list[dict[str, Any]] = []

    async def post(
        self,
        url: str,
        *,
        headers: dict[str, str],
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> _FakeResponse:
        _ = timeout
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "data": dict(data or {}),
                "files": dict(files or {}),
                "json": dict(json or {}),
            }
        )
        return self._responder(url, dict(data or {}), dict(json or {}), dict(files or {}))

    async def get(self, url: str, *, headers: dict[str, str], timeout: float | None = None) -> _FakeResponse:
        _ = (url, headers, timeout)
        return _FakeResponse(200, payload={"openapi": "3.1.0"})


@pytest.mark.asyncio
async def test_ut2_12_mineru_retries_read_errors_as_busy(monkeypatch) -> None:
    calls = {"count": 0}
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    def responder(
        _url: str, _data: dict[str, Any], _json_payload: dict[str, Any], _files: dict[str, Any]
    ) -> _FakeResponse:
        calls["count"] += 1
        if calls["count"] == 1:
            raise httpx.ReadError("socket reset")
        return _FakeResponse(200, payload={"results": {"doc": {"md_content": "Recovered after retry"}}})

    monkeypatch.setattr(mineru_module.asyncio, "sleep", fake_sleep)
    provider = MineruParserProvider(base_url="https://mineru.example.test")
    provider._client = _FakeMineruClient(responder)  # type: ignore[attr-defined]

    ir = await provider.parse_bytes(
        b"%PDF-1.4\nfake\n",
        filename="sample.pdf",
        source_uri="file://sample.pdf",
        mime_type="application/pdf",
    )

    assert "Recovered after retry" in ir.full_text()
    assert sleeps == [5.0]


@pytest.mark.asyncio
async def test_ut2_12_mineru_waits_for_busy_health_before_submit(monkeypatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    class _BusyThenReadyClient(_FakeMineruClient):
        def __init__(self) -> None:
            super().__init__(
                lambda _url, _data, _json_payload, _files: _FakeResponse(
                    200, payload={"results": {"doc": {"md_content": "Recovered after health wait"}}}
                )
            )
            self.health_calls = 0

        async def get(self, url: str, *, headers: dict[str, str], timeout: float | None = None) -> _FakeResponse:
            _ = (url, headers, timeout)
            self.health_calls += 1
            if self.health_calls == 1:
                return _FakeResponse(
                    200,
                    payload={
                        "status": "busy",
                        "inflight": 1,
                        "waiting": 1,
                        "max_concurrent": 1,
                        "queue_max": 3,
                    },
                )
            return _FakeResponse(
                200,
                payload={
                    "status": "ready",
                    "inflight": 0,
                    "waiting": 0,
                    "max_concurrent": 1,
                    "queue_max": 3,
                },
            )

    monkeypatch.setattr(mineru_module.asyncio, "sleep", fake_sleep)
    provider = MineruParserProvider(base_url="https://mineru.example.test")
    provider._client = _BusyThenReadyClient()  # type: ignore[attr-defined]

    ir = await provider.parse_bytes(
        b"%PDF-1.4\nfake\n",
        filename="sample.pdf",
        source_uri="file://sample.pdf",
        mime_type="application/pdf",
    )

    assert "Recovered after health wait" in ir.full_text()
    assert sleeps == [5.0]


@pytest.mark.asyncio
async def test_ut2_12_mineru_allows_submit_when_worker_busy_but_queue_has_capacity(monkeypatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    class _BusyButQueueOpenClient(_FakeMineruClient):
        def __init__(self) -> None:
            super().__init__(
                lambda _url, _data, _json_payload, _files: _FakeResponse(
                    200, payload={"results": {"doc": {"md_content": "Queued request accepted"}}}
                )
            )

        async def get(self, url: str, *, headers: dict[str, str], timeout: float | None = None) -> _FakeResponse:
            _ = (url, headers, timeout)
            return _FakeResponse(
                200,
                payload={
                    "status": "busy",
                    "inflight": 1,
                    "waiting": 0,
                    "max_concurrent": 1,
                    "queue_max": 3,
                },
            )

    monkeypatch.setattr(mineru_module.asyncio, "sleep", fake_sleep)
    provider = MineruParserProvider(base_url="https://mineru.example.test")
    provider._client = _BusyButQueueOpenClient()  # type: ignore[attr-defined]

    ir = await provider.parse_bytes(
        b"%PDF-1.4\nfake\n",
        filename="sample.pdf",
        source_uri="file://sample.pdf",
        mime_type="application/pdf",
    )

    assert "Queued request accepted" in ir.full_text()
    assert sleeps == []


@pytest.mark.asyncio
async def test_ut2_12_mineru_adaptive_retry_uses_low_vram_payload() -> None:
    calls = {"count": 0}

    def responder(
        _url: str, data: dict[str, Any], _json_payload: dict[str, Any], _files: dict[str, Any]
    ) -> _FakeResponse:
        calls["count"] += 1
        if calls["count"] == 1:
            return _FakeResponse(500, text="CUDA out of memory")
        payload = {
            "results": {
                "doc": {
                    "md_content": "Recovered parser output",
                }
            }
        }
        return _FakeResponse(200, payload=payload)

    provider = MineruParserProvider(base_url="https://mineru.example.test")
    provider._client = _FakeMineruClient(responder)  # type: ignore[attr-defined]

    ir = await provider.parse_bytes(
        b"%PDF-1.4\nfake\n",
        filename="sample.pdf",
        source_uri="file://sample.pdf",
        mime_type="application/pdf",
        options={"parse_backend": "hybrid-auto-engine", "parse_method": "auto"},
    )

    assert ir.provider_id == "mineru"
    assert "Recovered parser output" in ir.full_text()
    assert len(provider._client.calls) >= 2  # type: ignore[attr-defined]
    assert provider._client.calls[0]["data"].get("backend") == "hybrid-auto-engine"  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_ut2_12_mineru_page_fallback_after_busy_retry_budget_exhausted() -> None:
    pdf = b"%PDF-1.4\nfake\n"

    def responder(
        _url: str, data: dict[str, Any], _json_payload: dict[str, Any], _files: dict[str, Any]
    ) -> _FakeResponse:
        if "start_page_id" not in data:
            raise httpx.RemoteProtocolError("Server disconnected without sending a response.")
        page_idx = int(data.get("start_page_id", 0))
        payload = {"results": {"doc": {"md_content": f"page-{page_idx} busy fallback"}}}
        return _FakeResponse(200, payload=payload)

    provider = MineruParserProvider(base_url="https://mineru.example.test")
    provider._client = _FakeMineruClient(responder)  # type: ignore[attr-defined]
    original = mineru_module._pdf_page_count
    mineru_module._pdf_page_count = lambda _document: 2
    try:
        ir = await provider.parse_bytes(
            pdf,
            filename="two-pages.pdf",
            source_uri="file://two-pages.pdf",
            mime_type="application/pdf",
            options={"parse_backend": "pipeline", "parse_method": "auto"},
        )
    finally:
        mineru_module._pdf_page_count = original

    text = ir.full_text()
    assert "page-0 busy fallback" in text
    assert "page-1 busy fallback" in text
    assert ir.metadata.get("page_fallback") is True
    assert ir.metadata.get("target_chars") == 600
    assert ir.metadata.get("max_pages") == 3


@pytest.mark.asyncio
async def test_ut2_12_mineru_page_fallback_recovers_when_full_parse_oom() -> None:
    pdf = b"%PDF-1.4\nfake\n"

    def responder(
        _url: str, data: dict[str, Any], _json_payload: dict[str, Any], _files: dict[str, Any]
    ) -> _FakeResponse:
        if "start_page_id" not in data:
            return _FakeResponse(500, text="CUDA out of memory")
        page_idx = int(data.get("start_page_id", 0))
        payload = {"results": {"doc": {"md_content": f"page-{page_idx} text"}}}
        return _FakeResponse(200, payload=payload)

    provider = MineruParserProvider(base_url="https://mineru.example.test")
    provider._client = _FakeMineruClient(responder)  # type: ignore[attr-defined]
    original = mineru_module._pdf_page_count
    mineru_module._pdf_page_count = lambda _document: 2
    try:
        ir = await provider.parse_bytes(
            pdf,
            filename="two-pages.pdf",
            source_uri="file://two-pages.pdf",
            mime_type="application/pdf",
            options={
                "parse_backend": "hybrid-auto-engine",
                "parse_method": "auto",
                "page_fallback_target_chars": 0,
                "page_fallback_max_pages": 2,
            },
        )
    finally:
        mineru_module._pdf_page_count = original

    text = ir.full_text()
    assert "page-0 text" in text
    assert "page-1 text" in text
    assert ir.metadata.get("page_fallback") is True


@pytest.mark.asyncio
async def test_ut2_12_mineru_health_check_accepts_alternate_liveness_endpoint() -> None:
    class _FlakyHealthClient(_FakeMineruClient):
        async def get(self, url: str, *, headers: dict[str, str]) -> _FakeResponse:
            _ = headers
            if url.endswith("/openapi.json"):
                return _FakeResponse(503, text="temporarily unavailable")
            if url.endswith("/docs"):
                return _FakeResponse(200, text="ok")
            return _FakeResponse(404, text="not found")

    provider = MineruParserProvider(base_url="https://mineru.example.test")
    provider._client = _FlakyHealthClient(  # type: ignore[attr-defined]
        lambda _url, _data, _json_payload, _files: _FakeResponse(500, text="unused")
    )

    assert await provider.health_check() is True


@pytest.mark.asyncio
async def test_ut2_12_mineru_gradio_fallback_on_file_parse_404() -> None:
    def responder(
        url: str, data: dict[str, Any], json_payload: dict[str, Any], _files: dict[str, Any]
    ) -> _FakeResponse:
        if url.endswith("/file_parse"):
            return _FakeResponse(404, text="404 page not found")
        if url.endswith("/gradio_api/upload"):
            return _FakeResponse(200, payload=["/tmp/gradio/uploaded/sample.pdf"])
        if url.endswith("/gradio_api/run/to_markdown"):
            payload = {"data": ["# Markdown", "Recovered via gradio fallback"]}
            return _FakeResponse(200, payload=payload)
        return _FakeResponse(500, text=f"unexpected url {url}")

    provider = MineruParserProvider(base_url="https://mineru.example.test")
    provider._client = _FakeMineruClient(responder)  # type: ignore[attr-defined]

    ir = await provider.parse_bytes(
        b"%PDF-1.4\nfake\n",
        filename="sample.pdf",
        source_uri="file://sample.pdf",
        mime_type="application/pdf",
        options={"parse_backend": "pipeline", "parse_method": "auto"},
    )

    assert "Recovered via gradio fallback" in ir.full_text()
    called_urls = [entry["url"] for entry in provider._client.calls]  # type: ignore[attr-defined]
    assert any(url.endswith("/gradio_api/upload") for url in called_urls)
    assert any(url.endswith("/gradio_api/run/to_markdown") for url in called_urls)


@pytest.mark.asyncio
async def test_ut2_12_mineru_fail_fast_on_persistent_route_404() -> None:
    def responder(
        url: str, data: dict[str, Any], _json_payload: dict[str, Any], _files: dict[str, Any]
    ) -> _FakeResponse:
        if url.endswith("/file_parse"):
            return _FakeResponse(404, text="404 page not found")
        if url.endswith("/gradio_api/upload"):
            return _FakeResponse(404, text="404 page not found")
        if url.endswith("/gradio_api/run/to_markdown"):
            return _FakeResponse(404, text="404 page not found")
        return _FakeResponse(404, text="404 page not found")

    provider = MineruParserProvider(base_url="https://mineru.example.test")
    provider._client = _FakeMineruClient(responder)  # type: ignore[attr-defined]

    original = mineru_module._pdf_page_count
    mineru_module._pdf_page_count = lambda _document: (_ for _ in ()).throw(
        AssertionError("page fallback should not run")
    )
    try:
        with pytest.raises(Exception, match="endpoint unavailable"):
            await provider.parse_bytes(
                b"%PDF-1.4\nfake\n",
                filename="sample.pdf",
                source_uri="file://sample.pdf",
                mime_type="application/pdf",
                options={"parse_backend": "pipeline", "parse_method": "auto"},
            )
    finally:
        mineru_module._pdf_page_count = original
