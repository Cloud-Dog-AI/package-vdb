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
import base64
import binascii
import json as _json
from collections.abc import Callable
from typing import Any

import httpx

from cloud_dog_vdb.domain.errors import BackendUnavailableError, InvalidRequestError
from cloud_dog_vdb.ingestion.parse.async_runner import AsyncParseRunner
from cloud_dog_vdb.ingestion.parse.base import ParserProvider
from cloud_dog_vdb.ingestion.parse.capabilities import ParserCapabilities
from cloud_dog_vdb.ingestion.parse.ir import DocumentIR, TextBlock
from cloud_dog_vdb.ingestion.parse.providers.internal import _decode_best_effort, _to_text_blocks


def _coerce_marker_text(payload: dict[str, Any]) -> str:
    for key in ("output", "markdown", "text", "content"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    result = payload.get("result")
    if isinstance(result, dict):
        for key in ("output", "markdown", "text", "content"):
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                return value
    return ""


def _coerce_marker_images(payload: dict[str, Any]) -> list[dict[str, Any]]:
    images = payload.get("images")
    if not isinstance(images, dict):
        result = payload.get("result")
        if isinstance(result, dict):
            images = result.get("images")
    if not isinstance(images, dict):
        return []

    out: list[dict[str, Any]] = []
    for key, value in images.items():
        if not isinstance(key, str) or not key.strip():
            continue
        if not isinstance(value, str) or not value.strip():
            continue
        encoded = value
        if value.startswith("data:") and "," in value:
            encoded = value.split(",", 1)[1]
        try:
            decoded = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error):
            continue
        out.append(
            {
                "ref": key,
                "encoding": "base64",
                "mime_type": _guess_mime_type_from_ref(key),
                "data_base64": encoded,
                "byte_size": len(decoded),
            }
        )
    return out


def _coerce_marker_toc_blocks(payload: dict[str, Any]) -> list[TextBlock]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        result = payload.get("result")
        if isinstance(result, dict):
            metadata = result.get("metadata")
    if not isinstance(metadata, dict):
        return []

    table_of_contents = metadata.get("table_of_contents")
    if not isinstance(table_of_contents, list):
        return []

    out: list[TextBlock] = []
    for entry in table_of_contents:
        if not isinstance(entry, dict):
            continue
        title = str(entry.get("title", "")).strip()
        if not title:
            continue
        try:
            level = int(entry.get("level", 1) or 1)
        except (TypeError, ValueError):
            level = 1
        level = max(1, min(level, 6))
        page_value = entry.get("page_id")
        page: int | None = None
        if isinstance(page_value, int):
            page = page_value
        elif isinstance(page_value, str) and page_value.isdigit():
            page = int(page_value)
        out.append(
            TextBlock(
                text=f"{'#' * level} {title}",
                kind="heading",
                page=page,
                section_path=(title,),
            )
        )
    return out


def _guess_mime_type_from_ref(ref: str) -> str:
    lowered = ref.strip().lower()
    if lowered.endswith(".png"):
        return "image/png"
    if lowered.endswith(".webp"):
        return "image/webp"
    if lowered.endswith(".gif"):
        return "image/gif"
    return "image/jpeg"


def _parse_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def _parse_float(value: Any, *, default: float, minimum: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(parsed, minimum)


def _looks_like_busy_error(message: str) -> bool:
    text = str(message or "").strip().lower()
    if not text:
        return False
    markers = (
        "worker busy",
        "queue wait exceeded",
        "provider busy",
        "too many requests",
        "http 429",
    )
    return any(marker in text for marker in markers)


def _normalise_optional_url(base_url: str, value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if raw.startswith("/"):
        return f"{base_url}{raw}"
    return f"{base_url}/{raw}"


def _marker_supports_input(filename: str, mime_type: str | None) -> bool:
    allowed_mime_types = {
        "application/pdf",
        "application/x-pdf",
        "application/msword",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "text/html",
        "application/xhtml+xml",
    }
    allowed_extensions = {"pdf", "doc", "docx", "html", "htm"}

    resolved_mime = str(mime_type or "").strip().lower().split(";", 1)[0]
    if resolved_mime in allowed_mime_types:
        return True
    if resolved_mime.startswith("text/"):
        return False

    name = str(filename or "").strip().lower()
    if "." in name:
        extension = name.rsplit(".", 1)[1]
        if extension:
            return extension in allowed_extensions
    return True


def _parse_mcp_response(resp: Any) -> dict[str, Any]:
    """Extract JSON-RPC envelope from an MCP response (JSON or SSE)."""
    content_type = ""
    if hasattr(resp, "headers") and isinstance(resp.headers, dict):
        content_type = resp.headers.get("content-type", "")
    elif hasattr(resp, "headers"):
        content_type = str(getattr(resp.headers, "get", lambda k, d="": d)("content-type", ""))

    raw = resp.text if hasattr(resp, "text") and isinstance(resp.text, str) else ""

    # SSE: extract last data line containing a JSON-RPC result
    if "text/event-stream" in content_type or (raw and not raw.lstrip().startswith("{")):
        for line in reversed(raw.splitlines()):
            stripped = line.strip()
            if stripped.startswith("data:"):
                data = stripped[5:].strip()
                if data and data != "[DONE]":
                    try:
                        return _json.loads(data)
                    except _json.JSONDecodeError:
                        continue
        # Fallback: try the whole body as JSON
        try:
            return _json.loads(raw)
        except (_json.JSONDecodeError, ValueError):
            return {}

    # JSON: standard parse
    try:
        return resp.json()
    except Exception:
        try:
            return _json.loads(raw)
        except (_json.JSONDecodeError, ValueError):
            return {}


class MarkerMcpParserProvider(ParserProvider):
    """Represent marker mcp parser provider."""
    provider_id = "marker_mcp"
    provider_version = "api-0.1.0"

    def __init__(
        self,
        *,
        base_url: str,
        auth_token: str = "",
        timeout_seconds: float = 120.0,
        request_retries: int = 3,
        async_threshold_seconds: float = 30.0,
        async_poll_interval_seconds: float = 5.0,
        async_max_wait_seconds: float = 600.0,
        async_status_path: str = "",
        async_result_path: str = "",
        async_cancel_path: str = "",
        busy_retry_initial_seconds: float = 35.0,
        busy_retry_max_seconds: float = 420.0,
        busy_retry_max_delay_seconds: float = 60.0,
        busy_retry_backoff: float = 1.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self.timeout_seconds = max(float(timeout_seconds), 1.0)
        self.request_retries = max(int(request_retries), 1)
        self.async_threshold_seconds = max(float(async_threshold_seconds), 0.0)
        self.async_poll_interval_seconds = max(float(async_poll_interval_seconds), 0.1)
        self.async_max_wait_seconds = max(float(async_max_wait_seconds), 1.0)
        self.async_status_path = async_status_path
        self.async_result_path = async_result_path
        self.async_cancel_path = async_cancel_path
        self.busy_retry_initial_seconds = max(float(busy_retry_initial_seconds), 1.0)
        self.busy_retry_max_seconds = max(float(busy_retry_max_seconds), 0.0)
        self.busy_retry_max_delay_seconds = max(float(busy_retry_max_delay_seconds), 1.0)
        self.busy_retry_backoff = max(float(busy_retry_backoff), 1.0)
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    @property
    def capabilities(self) -> ParserCapabilities:
        """Handle capabilities."""
        return ParserCapabilities(
            supports_pdf=True,
            supports_docx=True,
            supports_html=True,
            supports_layout=True,
            supports_tables=True,
            supports_images=True,
            supports_ocr_passthrough=True,
            supports_streaming=False,
            max_document_bytes=128 * 1024 * 1024,
        )

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json, text/event-stream"}
        if self.auth_token:
            headers["X-API-Key"] = self.auth_token
        return headers

    async def _ensure_mcp_session(self, *, timeout: float | None = None) -> str:
        """Initialise an MCP session and return the session ID."""
        resp = await self._client.post(
            f"{self.base_url}/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "cloud_dog_vdb", "version": "0.1.0"},
                },
            },
            headers=self._headers(),
            timeout=timeout,
        )
        if resp.status_code >= 400:
            raise BackendUnavailableError(f"MCP initialize failed: {resp.status_code} {resp.text[:200]}")
        session_id = resp.headers.get("mcp-session-id", "")
        # Consume the response body (may be JSON or SSE) to complete the request
        _parse_mcp_response(resp)
        # Send initialized notification (fire-and-forget)
        notify_headers = dict(self._headers())
        if session_id:
            notify_headers["Mcp-Session-Id"] = session_id
        try:
            await self._client.post(
                f"{self.base_url}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized",
                    "params": {},
                },
                headers=notify_headers,
                timeout=timeout,
            )
        except Exception:
            pass
        return session_id

    async def _call_mcp_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        session_id: str = "",
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Call an MCP tool via JSON-RPC and return the parsed result dict."""
        headers = self._headers()
        if session_id:
            headers["Mcp-Session-Id"] = session_id
        resp = await self._client.post(
            f"{self.base_url}/mcp",
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": arguments},
            },
            headers=headers,
            timeout=timeout,
        )
        if resp.status_code >= 400:
            raise InvalidRequestError(f"MCP tool call failed: {resp.status_code} {resp.text[:200]}")
        envelope = _parse_mcp_response(resp)
        if "error" in envelope:
            err = envelope["error"]
            msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
            raise InvalidRequestError(f"MCP tool error: {msg}")
        result = envelope.get("result", {})
        if result.get("isError"):
            content = result.get("content", [])
            err_text = content[0].get("text", "unknown") if content else "unknown"
            raise InvalidRequestError(f"marker_mcp tool error: {err_text}")
        content = result.get("content", [])
        if content and isinstance(content[0], dict) and content[0].get("type") == "text":
            try:
                return _json.loads(content[0]["text"])
            except (_json.JSONDecodeError, KeyError):
                return {"output": content[0].get("text", "")}
        return result

    async def health_check(self) -> bool:
        """Handle health check."""
        probe_paths = ("/health", "/")
        for path in probe_paths:
            for _ in range(2):
                try:
                    resp = await self._client.get(f"{self.base_url}{path}", headers=self._headers())
                    if resp.status_code < 500:
                        return True
                except Exception:
                    continue
        return False

    @staticmethod
    def _build_form_options(options: dict[str, Any]) -> dict[str, Any]:
        output_format = str(options.get("output_format", "markdown") or "markdown")
        paginate_output = _parse_bool(options.get("paginate_output"), default=True)
        form: dict[str, Any] = {
            "output_format": output_format,
            "paginate_output": "true" if paginate_output else "false",
        }
        page_range = options.get("page_range")
        if page_range is not None and str(page_range).strip():
            form["page_range"] = str(page_range)
        return form

    def _async_url(self, explicit: str, configured_path: str) -> str:
        if explicit:
            return _normalise_optional_url(self.base_url, explicit)
        return _normalise_optional_url(self.base_url, configured_path)

    async def _parse_sync_payload(
        self,
        *,
        document: bytes,
        filename: str,
        mime_type: str | None,
        form_options: dict[str, Any],
        total_timeout_seconds: float,
    ) -> dict[str, Any]:
        timeout_budget = max(float(total_timeout_seconds), 1.0)
        loop = asyncio.get_running_loop()
        started_at = loop.time()

        def remaining_budget() -> float:
            """Handle remaining budget."""
            return timeout_budget - (loop.time() - started_at)

        remaining = remaining_budget()
        if remaining <= 0:
            raise InvalidRequestError(f"marker_mcp parse exceeded {timeout_budget:.1f}s timeout budget")
        session_id = await self._ensure_mcp_session(timeout=max(remaining, 1.0))
        file_base64 = base64.b64encode(document).decode("ascii")
        arguments: dict[str, Any] = {
            "file_base64": file_base64,
            "filename": filename,
            "output_format": form_options.get("output_format", "markdown"),
            "paginate_output": form_options.get("paginate_output", "false") == "true",
        }
        page_range = form_options.get("page_range")
        if page_range is not None and str(page_range).strip():
            arguments["page_range"] = str(page_range)

        last_exc: Exception | None = None
        attempt = 0
        busy_wait_elapsed = 0.0
        busy_delay = self.busy_retry_initial_seconds
        while True:
            remaining = remaining_budget()
            if remaining <= 0:
                raise InvalidRequestError(
                    f"marker_mcp parse exceeded {timeout_budget:.1f}s timeout budget"
                ) from last_exc
            try:
                call_timeout = max(remaining, 1.0)
                payload = await asyncio.wait_for(
                    self._call_mcp_tool(
                        "marker_convert_pdf_base64",
                        arguments,
                        session_id=session_id,
                        timeout=call_timeout,
                    ),
                    timeout=call_timeout,
                )
                if payload.get("success") is False:
                    raise InvalidRequestError(f"marker_mcp parse failed: {payload.get('error', 'unknown_error')}")
                return payload
            except asyncio.TimeoutError as exc:
                last_exc = exc
                raise InvalidRequestError(
                    f"marker_mcp parse exceeded {timeout_budget:.1f}s timeout budget during tools/call"
                ) from exc
            except InvalidRequestError as exc:
                last_exc = exc
                error_message = str(exc)
                if _looks_like_busy_error(error_message):
                    if busy_wait_elapsed >= min(self.busy_retry_max_seconds, timeout_budget):
                        raise InvalidRequestError(
                            f"marker_mcp parse failed: busy retries exhausted after {busy_wait_elapsed:.1f}s"
                        ) from exc
                    remaining = remaining_budget()
                    if remaining <= 0:
                        raise InvalidRequestError(
                            f"marker_mcp parse exceeded {timeout_budget:.1f}s timeout budget"
                        ) from exc
                    wait_for = min(busy_delay, self.busy_retry_max_delay_seconds)
                    remaining_busy = max(min(self.busy_retry_max_seconds, timeout_budget) - busy_wait_elapsed, 0.0)
                    wait_for = min(wait_for, max(remaining_busy, 0.0), max(remaining, 0.0))
                    if wait_for < 1.0:
                        raise InvalidRequestError(
                            f"marker_mcp parse exceeded {timeout_budget:.1f}s timeout budget"
                        ) from exc
                    await asyncio.sleep(wait_for)
                    busy_wait_elapsed += wait_for
                    busy_delay = min(busy_delay * self.busy_retry_backoff, self.busy_retry_max_delay_seconds)
                    continue
                # Do not retry terminal parser/tool errors (for example server-side OCR timeout).
                raise
            except httpx.HTTPError as exc:
                last_exc = exc
                attempt += 1
                if attempt < self.request_retries:
                    remaining = remaining_budget()
                    delay = min(0.5 * attempt, max(remaining, 0.0))
                    if delay > 0:
                        await asyncio.sleep(delay)
                    continue
                raise

        raise InvalidRequestError(f"marker_mcp parse failed after {self.request_retries} attempts: {last_exc}")

    def _resolve_async_mode(self, document: bytes, options: dict[str, Any]) -> tuple[bool, float]:
        estimated_override = options.get("estimated_parse_time_seconds")
        threshold = _parse_float(
            options.get("async_threshold_seconds", self.async_threshold_seconds),
            default=self.async_threshold_seconds,
            minimum=0.0,
        )
        if estimated_override is not None:
            estimated_seconds = _parse_float(estimated_override, default=0.0, minimum=0.0)
        else:
            estimated_bytes_per_second = _parse_float(
                options.get("estimated_bytes_per_second", 250_000.0),
                default=250_000.0,
                minimum=1.0,
            )
            estimated_seconds = float(len(document)) / estimated_bytes_per_second

        explicit = options.get("async_mode")
        if explicit is None:
            return estimated_seconds > threshold, estimated_seconds
        return _parse_bool(explicit, default=False), estimated_seconds

    async def parse_bytes(
        self,
        document: bytes,
        *,
        filename: str,
        source_uri: str,
        mime_type: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> DocumentIR:
        """Parse bytes."""
        if not self.base_url:
            raise BackendUnavailableError("marker_mcp base_url is not configured")
        if not _marker_supports_input(filename, mime_type):
            raise InvalidRequestError(
                f"marker_mcp unsupported document type for '{filename or 'unnamed'}' ({mime_type or 'unknown'})"
            )
        opts = dict(options or {})
        form_options = self._build_form_options(opts)
        request_timeout = _parse_float(
            opts.get("timeout_seconds", self.timeout_seconds),
            default=self.timeout_seconds,
            minimum=1.0,
        )
        use_async_mode, estimated_seconds = self._resolve_async_mode(document, opts)

        payload: dict[str, Any]
        execution_mode = "sync"
        if use_async_mode:
            submit_url = _normalise_optional_url(
                self.base_url,
                str(opts.get("async_submit_url", "/marker/upload")),
            )
            status_url = self._async_url(str(opts.get("async_status_url", "")), self.async_status_path)
            result_url = self._async_url(str(opts.get("async_result_url", "")), self.async_result_path)
            cancel_url = self._async_url(str(opts.get("async_cancel_url", "")), self.async_cancel_path)

            poll_interval = _parse_float(
                opts.get("async_poll_interval_seconds", self.async_poll_interval_seconds),
                default=self.async_poll_interval_seconds,
                minimum=0.1,
            )
            max_wait = _parse_float(
                opts.get("async_max_wait_seconds", self.async_max_wait_seconds),
                default=self.async_max_wait_seconds,
                minimum=1.0,
            )
            request_timeout = _parse_float(
                opts.get("timeout_seconds", self.timeout_seconds),
                default=self.timeout_seconds,
                minimum=1.0,
            )
            callback = opts.get("progress_callback")
            progress_callback: Callable[[dict[str, Any]], None] | None = None
            if callable(callback):
                progress_callback = callback

            runner = AsyncParseRunner(
                self._client,
                poll_interval=poll_interval,
                max_wait=max_wait,
                timeout=request_timeout,
                progress_callback=progress_callback,
            )
            payload = await runner.run(
                submit_url,
                document,
                filename,
                headers=self._headers(),
                submit_data=form_options,
                status_url=status_url or None,
                result_url=result_url or None,
                cancel_url=cancel_url or None,
                sync_fallback=lambda: self._parse_sync_payload(
                    document=document,
                    filename=filename,
                    mime_type=mime_type,
                    form_options=form_options,
                    total_timeout_seconds=request_timeout,
                ),
            )
            execution_mode = "async" if status_url and result_url else "sync_fallback"
        else:
            payload = await self._parse_sync_payload(
                document=document,
                filename=filename,
                mime_type=mime_type,
                form_options=form_options,
                total_timeout_seconds=request_timeout,
            )

        text = _coerce_marker_text(payload).strip()
        if not text:
            text = _decode_best_effort(document).strip()
        heading_blocks = _coerce_marker_toc_blocks(payload)
        text_blocks = heading_blocks + _to_text_blocks(text)
        artefact_refs = _coerce_marker_images(payload)
        return DocumentIR(
            source_uri=source_uri,
            provider_id=self.provider_id,
            provider_version=self.provider_version,
            text_blocks=text_blocks,
            artefact_refs=artefact_refs,
            metadata={"toc_count": len(heading_blocks), "execution_mode": execution_mode},
            quality={"confidence": 0.85, "images_count": float(len(artefact_refs))},
        )
