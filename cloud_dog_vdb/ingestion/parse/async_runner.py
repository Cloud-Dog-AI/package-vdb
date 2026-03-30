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
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx

from cloud_dog_vdb.domain.errors import InvalidRequestError, ParserTimeoutError

ProgressCallback = Callable[[dict[str, Any]], None]
SyncFallback = Callable[[], Awaitable[dict[str, Any]]]


@dataclass(frozen=True, slots=True)
class AsyncParseConfig:
    """Represent async parse config."""
    poll_interval_seconds: float = 5.0
    max_wait_seconds: float = 600.0
    timeout_seconds: float = 120.0


class AsyncParseRunner:
    """Reusable submit/poll/retrieve wrapper for parser backends."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        poll_interval: float = 5.0,
        max_wait: float = 600.0,
        timeout: float = 120.0,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        self.client = client
        self.poll_interval = max(0.1, float(poll_interval))
        self.max_wait = max(1.0, float(max_wait))
        self.timeout = max(1.0, float(timeout))
        self.progress_callback = progress_callback

    def _emit(self, event: dict[str, Any]) -> None:
        if self.progress_callback is None:
            return
        self.progress_callback(dict(event))

    async def submit(
        self,
        submit_url: str,
        document: bytes,
        filename: str,
        *,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        file_field: str = "file",
    ) -> str:
        """Handle submit."""
        files = {file_field: (filename, document, "application/octet-stream")}
        response = await self.client.post(
            submit_url,
            headers=headers,
            data=data,
            files=files,
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise InvalidRequestError(f"async submit failed: {response.status_code} {response.text[:240]}")
        payload = response.json()
        for key in ("job_id", "request_id", "id"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
            if isinstance(value, int):
                return str(value)
        result = payload.get("result")
        if isinstance(result, dict):
            for key in ("job_id", "request_id", "id"):
                value = result.get(key)
                if isinstance(value, str) and value.strip():
                    return value
                if isinstance(value, int):
                    return str(value)
        raise InvalidRequestError("async submit returned no job identifier")

    async def poll(self, status_url: str, job_id: str, *, headers: dict[str, str] | None = None) -> dict[str, Any]:
        """Handle poll."""
        url = status_url.format(job_id=job_id)
        response = await self.client.get(url, headers=headers, timeout=self.timeout)
        if response.status_code >= 400:
            raise InvalidRequestError(f"async poll failed: {response.status_code} {response.text[:240]}")
        payload = response.json()
        if not isinstance(payload, dict):
            raise InvalidRequestError("async poll returned non-object payload")
        return payload

    async def retrieve(
        self,
        result_url: str,
        job_id: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Handle retrieve."""
        url = result_url.format(job_id=job_id)
        response = await self.client.get(url, headers=headers, timeout=self.timeout)
        if response.status_code >= 400:
            raise InvalidRequestError(f"async retrieve failed: {response.status_code} {response.text[:240]}")
        payload = response.json()
        if not isinstance(payload, dict):
            raise InvalidRequestError("async retrieve returned non-object payload")
        return payload

    async def cancel(
        self,
        cancel_url: str,
        job_id: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Handle cancel."""
        url = cancel_url.format(job_id=job_id)
        response = await self.client.post(url, headers=headers, timeout=self.timeout)
        if response.status_code >= 400:
            raise InvalidRequestError(f"async cancel failed: {response.status_code} {response.text[:240]}")
        payload = response.json()
        if not isinstance(payload, dict):
            return {}
        return payload

    @staticmethod
    def _is_terminal(status_payload: dict[str, Any]) -> tuple[bool, bool]:
        status = str(status_payload.get("status", "")).strip().lower()
        completed = bool(status_payload.get("completed", False))
        succeeded = bool(status_payload.get("success", False))

        if status in {"failed", "error", "cancelled", "canceled"}:
            return True, False
        if status in {"complete", "completed", "done", "success", "succeeded"}:
            return True, True
        if completed and succeeded:
            return True, True
        if completed and not succeeded and "error" in status_payload:
            return True, False
        return False, False

    async def run(
        self,
        submit_url: str,
        document: bytes,
        filename: str,
        *,
        headers: dict[str, str] | None = None,
        submit_data: dict[str, Any] | None = None,
        status_url: str | None = None,
        result_url: str | None = None,
        cancel_url: str | None = None,
        sync_fallback: SyncFallback | None = None,
    ) -> dict[str, Any]:
        """Handle run."""
        if not status_url or not result_url:
            if sync_fallback is None:
                raise InvalidRequestError("async mode requested without status/result endpoints")
            self._emit({"event": "heartbeat", "mode": "sync_fallback", "phase": "start"})
            payload = await sync_fallback()
            self._emit({"event": "heartbeat", "mode": "sync_fallback", "phase": "done"})
            return payload

        job_id = await self.submit(
            submit_url,
            document,
            filename,
            headers=headers,
            data=submit_data,
        )

        start = time.monotonic()
        while True:
            payload = await self.poll(status_url, job_id, headers=headers)
            self._emit({"event": "poll", "job_id": job_id, "status": payload.get("status")})

            terminal, success = self._is_terminal(payload)
            if terminal:
                if not success:
                    message = str(payload.get("error") or payload.get("message") or "async parse failed")
                    raise InvalidRequestError(message)
                if "output" in payload or "markdown" in payload or "text" in payload:
                    return payload
                return await self.retrieve(result_url, job_id, headers=headers)

            elapsed = time.monotonic() - start
            if elapsed >= self.max_wait:
                if cancel_url:
                    try:
                        await self.cancel(cancel_url, job_id, headers=headers)
                    except Exception:
                        pass
                raise ParserTimeoutError(f"async parse timed out after {self.max_wait:.1f}s")

            await asyncio.sleep(self.poll_interval)
