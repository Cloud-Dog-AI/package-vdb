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
import io
import json
from typing import Any

import httpx

from cloud_dog_vdb.domain.errors import BackendUnavailableError, InvalidRequestError
from cloud_dog_vdb.ingestion.parse.base import ParserProvider
from cloud_dog_vdb.ingestion.parse.capabilities import ParserCapabilities
from cloud_dog_vdb.ingestion.parse.ir import DocumentIR, TextBlock
from cloud_dog_vdb.ingestion.parse.providers.internal import _decode_best_effort, _to_text_blocks

_MINERU_SEMAPHORES: dict[tuple[str, int], asyncio.Semaphore] = {}


def _coerce_mineru_text(body: dict[str, Any]) -> str:
    if isinstance(body.get("text"), str):
        return str(body["text"])
    if isinstance(body.get("markdown"), str):
        return str(body["markdown"])
    results = body.get("results")
    if isinstance(results, dict) and results:
        first = next(iter(results.values()))
        if isinstance(first, dict):
            for key in ("md_content", "markdown", "text", "content"):
                value = first.get(key)
                if isinstance(value, str) and value.strip():
                    return value
    for key in ("content", "output"):
        value = body.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def _parse_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def _parse_int(value: Any, *, default: int, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(parsed, minimum)


def _parse_float(value: Any, *, default: float, minimum: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(parsed, minimum)


def _looks_like_oom(message: str) -> bool:
    text = message.lower()
    return "cuda out of memory" in text or "out of memory" in text


def _looks_like_timeout(message: str) -> bool:
    text = message.lower()
    return "readtimeout" in text or "connecttimeout" in text or "timed out" in text


def _looks_like_busy_error(message: str) -> bool:
    text = message.lower()
    markers = (
        "worker busy",
        "provider busy",
        "too many requests",
        "queue wait exceeded",
        "http 429",
        "http 503",
        "readerror",
        "connecterror",
        "connecttimeout",
        "readtimeout",
        "pooltimeout",
        "remoteprotocolerror",
        "service unavailable",
        "connection reset",
        "connection refused",
    )
    return any(marker in text for marker in markers)


def _looks_like_not_found(message: str) -> bool:
    text = message.lower()
    return " 404 " in f" {text} " or "404 page not found" in text


def _supports_page_fallback(filename: str, mime_type: str | None) -> bool:
    name = filename.lower()
    if name.endswith(".pdf"):
        return True
    return str(mime_type or "").lower().strip() == "application/pdf"


def _health_state_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    status = str(payload.get("status", "")).strip().lower()
    inflight = _parse_int(payload.get("inflight"), default=0, minimum=0)
    waiting = _parse_int(payload.get("waiting"), default=0, minimum=0)
    max_concurrent = _parse_int(payload.get("max_concurrent"), default=0, minimum=0)
    queue_max = _parse_int(payload.get("queue_max"), default=0, minimum=0)
    queue_has_capacity = queue_max > 0 and waiting < queue_max
    busy = status in {"overloaded", "degraded_busy"}
    if waiting > 0:
        busy = True
    elif max_concurrent > 0 and inflight >= max_concurrent and not queue_has_capacity:
        busy = True
    detail = (
        f"status={status or 'unknown'} inflight={inflight} waiting={waiting} "
        f"max_concurrent={max_concurrent} queue_max={queue_max}"
    )
    return {
        "state": "busy" if busy else "ready",
        "detail": detail,
        "status": status or "unknown",
        "inflight": inflight,
        "waiting": waiting,
        "max_concurrent": max_concurrent,
        "queue_max": queue_max,
    }


def _shared_semaphore(base_url: str, limit: int) -> asyncio.Semaphore:
    key = (base_url.rstrip("/"), max(int(limit), 1))
    semaphore = _MINERU_SEMAPHORES.get(key)
    if semaphore is None:
        semaphore = asyncio.Semaphore(key[1])
        _MINERU_SEMAPHORES[key] = semaphore
    return semaphore


def _pdf_page_count(document: bytes) -> int | None:
    try:
        from pypdf import PdfReader
    except Exception:
        return None
    try:
        reader = PdfReader(io.BytesIO(document))
        return len(reader.pages)
    except Exception:
        return None


class MineruParserProvider(ParserProvider):
    """Represent mineru parser provider."""

    provider_id = "mineru"
    provider_version = "api-0.1.0"

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str = "",
        timeout_seconds: float = 120.0,
        parse_backend: str = "pipeline",
        parse_method: str = "txt",
        request_retries: int = 3,
        busy_retry_attempts: int = 3,
        busy_retry_initial_seconds: float = 5.0,
        busy_retry_max_delay_seconds: float = 20.0,
        busy_retry_backoff: float = 2.0,
        health_path: str = "/health",
        health_timeout_seconds: float = 8.0,
        max_concurrent_requests: int = 1,
        probe_health_before_parse: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = max(float(timeout_seconds), 1.0)
        self.parse_backend = parse_backend
        self.parse_method = parse_method
        self.request_retries = max(int(request_retries), 1)
        self.busy_retry_attempts = max(int(busy_retry_attempts), 0)
        self.busy_retry_initial_seconds = max(float(busy_retry_initial_seconds), 1.0)
        self.busy_retry_max_delay_seconds = max(float(busy_retry_max_delay_seconds), 1.0)
        self.busy_retry_backoff = max(float(busy_retry_backoff), 1.0)
        self.health_path = "/" + str(health_path or "/health").lstrip("/")
        self.health_timeout_seconds = max(float(health_timeout_seconds), 1.0)
        self.max_concurrent_requests = max(int(max_concurrent_requests), 1)
        self.probe_health_before_parse = bool(probe_health_before_parse)
        self._client = httpx.AsyncClient(timeout=self.timeout_seconds)
        self._semaphore = _shared_semaphore(self.base_url, self.max_concurrent_requests)

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
            max_document_bytes=256 * 1024 * 1024,
        )

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    @staticmethod
    def _gradio_base_candidates(base_url: str) -> list[str]:
        out = [base_url.rstrip("/")]
        if "mineruapi" in base_url:
            candidate = base_url.replace("mineruapi", "minerugui").rstrip("/")
            if candidate not in out:
                out.append(candidate)
        return out

    async def _request_parse(
        self,
        *,
        document: bytes,
        filename: str,
        mime_type: str | None,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        files = {"files": (filename, document, mime_type or "application/octet-stream")}
        retries = self.request_retries
        resp: httpx.Response | None = None
        attempt = 0
        busy_attempt = 0
        busy_delay = self.busy_retry_initial_seconds
        while True:
            try:
                resp = await self._client.post(
                    f"{self.base_url}/file_parse",
                    headers=self._headers(),
                    data=payload,
                    files=files,
                )
            except httpx.HTTPError as exc:
                attempt += 1
                error_message = f"{type(exc).__name__}: {exc}"
                if _looks_like_busy_error(error_message):
                    if busy_attempt >= self.busy_retry_attempts:
                        raise InvalidRequestError(f"mineru parse failed: busy retries exhausted; {error_message}") from exc
                    await asyncio.sleep(min(busy_delay, self.busy_retry_max_delay_seconds))
                    busy_attempt += 1
                    busy_delay = min(
                        busy_delay * self.busy_retry_backoff,
                        self.busy_retry_max_delay_seconds,
                    )
                    continue
                if attempt < retries:
                    await asyncio.sleep(0.5 * attempt)
                    continue
                raise InvalidRequestError(f"mineru parse request failed: {type(exc).__name__}: {exc}") from exc

            attempt += 1
            status_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
            if resp.status_code in {429, 503} or (resp.status_code >= 500 and _looks_like_busy_error(status_error)):
                if busy_attempt >= self.busy_retry_attempts:
                    raise InvalidRequestError(f"mineru parse failed: busy retries exhausted; {status_error}")
                await asyncio.sleep(min(busy_delay, self.busy_retry_max_delay_seconds))
                busy_attempt += 1
                busy_delay = min(
                    busy_delay * self.busy_retry_backoff,
                    self.busy_retry_max_delay_seconds,
                )
                continue
            if resp.status_code >= 500 and attempt < retries:
                await asyncio.sleep(0.5 * attempt)
                continue
            break

        if resp is None:
            raise InvalidRequestError("mineru parse request failed: no response")
        if resp.status_code >= 400:
            if resp.status_code == 404:
                # MinerU deployments may expose Gradio API paths instead of /file_parse.
                fallback_errors: list[str] = []
                original_error = f"{resp.status_code} {resp.text[:300]}"
                for candidate_base in self._gradio_base_candidates(self.base_url):
                    try:
                        return await self._request_parse_via_gradio(
                            document=document,
                            filename=filename,
                            mime_type=mime_type,
                            payload=payload,
                            original_error=original_error,
                            base_url_override=candidate_base,
                        )
                    except InvalidRequestError as exc:
                        fallback_errors.append(f"{candidate_base}: {exc}")
                detail = " | ".join(fallback_errors) if fallback_errors else "no fallback candidates"
                raise InvalidRequestError(f"mineru parse failed: {original_error}; gradio fallback failed: {detail}")
            raise InvalidRequestError(f"mineru parse failed: {resp.status_code} {resp.text[:300]}")
        try:
            out = resp.json()
        except Exception as exc:
            raise InvalidRequestError(f"mineru parse returned non-json response: {exc}") from exc
        return out

    async def _health_state(self) -> dict[str, Any]:
        try:
            resp = await self._client.get(
                f"{self.base_url}{self.health_path}",
                headers=self._headers(),
                timeout=self.health_timeout_seconds,
            )
        except httpx.HTTPError as exc:
            return {"state": "unknown", "detail": f"{type(exc).__name__}: {exc}"}

        if resp.status_code >= 500:
            return {"state": "unknown", "detail": f"http_{resp.status_code}"}
        if resp.status_code >= 400:
            return {"state": "unknown", "detail": f"health_http_{resp.status_code}"}
        try:
            if hasattr(resp, "content"):
                payload = resp.json() if resp.content else {}
            else:
                payload = resp.json()
        except Exception as exc:
            return {"state": "unknown", "detail": f"invalid_json: {exc}"}
        if not isinstance(payload, dict):
            return {"state": "unknown", "detail": "invalid_payload"}
        return _health_state_from_payload(payload)

    async def _wait_for_capacity(self) -> None:
        if not self.probe_health_before_parse:
            return

        attempts = self.busy_retry_attempts + 1
        delay = self.busy_retry_initial_seconds
        last_state: dict[str, Any] | None = None
        for attempt in range(attempts):
            state = await self._health_state()
            last_state = state
            if state.get("state") != "busy":
                return
            if attempt + 1 >= attempts:
                break
            await asyncio.sleep(min(delay, self.busy_retry_max_delay_seconds))
            delay = min(delay * self.busy_retry_backoff, self.busy_retry_max_delay_seconds)

        # Treat the health probe as advisory rather than a hard gate.
        # If the endpoint remains busy, fall through to the real request path
        # and let the request-level busy retry/page-fallback logic decide.
        return

    async def _request_parse_via_gradio(
        self,
        *,
        document: bytes,
        filename: str,
        mime_type: str | None,
        payload: dict[str, Any],
        original_error: str,
        base_url_override: str | None = None,
    ) -> dict[str, Any]:
        target_base_url = (base_url_override or self.base_url).rstrip("/")
        upload_files = {"files": (filename, document, mime_type or "application/octet-stream")}
        try:
            upload_resp = await self._client.post(
                f"{target_base_url}/gradio_api/upload",
                headers=self._headers(),
                data={},
                files=upload_files,
            )
        except httpx.HTTPError as exc:
            raise InvalidRequestError(f"mineru parse failed: {original_error}; gradio upload failed: {exc}") from exc

        if upload_resp.status_code >= 400:
            raise InvalidRequestError(
                "mineru parse failed: "
                f"{original_error}; gradio upload failed: {upload_resp.status_code} {upload_resp.text[:300]}"
            )

        try:
            upload_payload = upload_resp.json()
        except json.JSONDecodeError as exc:
            raise InvalidRequestError(
                f"mineru parse failed: {original_error}; gradio upload returned non-json: {exc}"
            ) from exc

        remote_path = ""
        if isinstance(upload_payload, list) and upload_payload:
            first = upload_payload[0]
            if isinstance(first, str):
                remote_path = first
        elif isinstance(upload_payload, dict):
            candidate = upload_payload.get("path")
            if isinstance(candidate, str):
                remote_path = candidate
        if not remote_path:
            raise InvalidRequestError(
                f"mineru parse failed: {original_error}; gradio upload response missing file path"
            )

        end_pages = 1000
        if "end_page_id" in payload:
            end_pages = max(int(payload.get("end_page_id", 0)) + 1, 1)
        is_ocr = str(payload.get("parse_method", "")).strip().lower() == "ocr"
        formula_enable = bool(payload.get("formula_enable", True))
        table_enable = bool(payload.get("table_enable", True))
        language = str(payload.get("lang_list", "en (English)")) or "en (English)"
        backend = str(payload.get("backend", "pipeline")) or "pipeline"
        server_url = str(payload.get("server_url", "http://localhost:30000")) or "http://localhost:30000"

        run_payload = {
            "data": [
                {"path": remote_path, "meta": {"_type": "gradio.FileData"}},
                end_pages,
                is_ocr,
                formula_enable,
                table_enable,
                language,
                backend,
                server_url,
            ]
        }
        try:
            run_resp = await self._client.post(
                f"{target_base_url}/gradio_api/run/to_markdown",
                headers=self._headers(),
                json=run_payload,
            )
        except httpx.HTTPError as exc:
            raise InvalidRequestError(f"mineru parse failed: {original_error}; gradio run failed: {exc}") from exc

        if run_resp.status_code >= 400:
            raise InvalidRequestError(
                "mineru parse failed: "
                f"{original_error}; gradio run failed: {run_resp.status_code} {run_resp.text[:300]}"
            )

        try:
            run_json = run_resp.json()
        except json.JSONDecodeError as exc:
            raise InvalidRequestError(
                f"mineru parse failed: {original_error}; gradio run returned non-json: {exc}"
            ) from exc

        text = ""
        if isinstance(run_json, dict):
            data = run_json.get("data")
            if isinstance(data, list):
                if len(data) > 1 and isinstance(data[1], str) and data[1].strip():
                    text = data[1]
                elif data and isinstance(data[0], str) and data[0].strip():
                    text = data[0]
        if not text.strip():
            raise InvalidRequestError(f"mineru parse failed: {original_error}; gradio run returned empty content")
        return {"markdown": text}

    @staticmethod
    def _extract_request_payload(
        parse_backend: str,
        parse_method: str,
        options: dict[str, Any],
    ) -> dict[str, Any]:
        allowed_keys = (
            "output_dir",
            "lang_list",
            "formula_enable",
            "table_enable",
            "server_url",
            "return_md",
            "return_middle_json",
            "return_model_output",
            "return_content_list",
            "return_images",
            "response_format_zip",
            "start_page_id",
            "end_page_id",
        )
        payload: dict[str, Any] = {"backend": parse_backend, "parse_method": parse_method}
        for key in allowed_keys:
            value = options.get(key)
            if value is None:
                continue
            payload[key] = value
        return payload

    @staticmethod
    def _adaptive_payloads(base_payload: dict[str, Any]) -> list[dict[str, Any]]:
        base = dict(base_payload)
        out = [base]

        low_vram = dict(base)
        low_vram["formula_enable"] = False
        low_vram["table_enable"] = False
        low_vram["return_middle_json"] = False
        low_vram["return_images"] = False
        if low_vram not in out:
            out.append(low_vram)

        for backend, method in (
            ("pipeline", "auto"),
            ("pipeline", "ocr"),
            ("vlm-auto-engine", "auto"),
            ("hybrid-auto-engine", "auto"),
        ):
            candidate = dict(low_vram)
            candidate["backend"] = backend
            candidate["parse_method"] = method
            if candidate not in out:
                out.append(candidate)
        return out

    async def _parse_pages(
        self,
        *,
        document: bytes,
        filename: str,
        source_uri: str,
        mime_type: str | None,
        payload_candidates: list[dict[str, Any]],
        target_chars: int,
        max_pages: int,
    ) -> DocumentIR:
        page_count = _pdf_page_count(document)
        if page_count is None or page_count <= 0:
            raise InvalidRequestError("mineru page fallback unavailable: unable to determine PDF page count")

        upper_bound = page_count if max_pages <= 0 else min(page_count, max_pages)
        pieces: list[str] = []
        errors: list[str] = []
        parsed_pages: list[int] = []

        for page_index in range(upper_bound):
            page_success = False
            for payload in payload_candidates[:2]:
                page_payload = dict(payload)
                page_payload["start_page_id"] = page_index
                page_payload["end_page_id"] = page_index
                try:
                    response = await self._request_parse(
                        document=document,
                        filename=filename,
                        mime_type=mime_type,
                        payload=page_payload,
                    )
                    text = _coerce_mineru_text(response).strip()
                    if text:
                        pieces.append(text)
                    parsed_pages.append(page_index)
                    page_success = True
                    break
                except InvalidRequestError as exc:
                    errors.append(f"page={page_index}:{exc}")
            if target_chars > 0 and len("\n\n".join(pieces)) >= target_chars:
                break
            if not page_success and page_index > 0 and not pieces:
                break

        text = "\n\n".join(piece for piece in pieces if piece.strip()).strip()
        if not text:
            detail = errors[0] if errors else "unknown error"
            raise InvalidRequestError(f"mineru page fallback failed: {detail}")

        return DocumentIR(
            source_uri=source_uri,
            provider_id=self.provider_id,
            provider_version=self.provider_version,
            text_blocks=_to_text_blocks(text) or [TextBlock(text=text)],
            quality={"confidence": 0.86},
            metadata={
                "page_fallback": True,
                "parsed_page_count": len(parsed_pages),
                "target_chars": target_chars,
                "max_pages": max_pages,
            },
        )

    async def health_check(self) -> bool:
        """Handle health check."""
        probe_paths = ("/openapi.json", "/docs", "/")
        acceptable_status_codes = {200, 204, 301, 302, 307, 308, 401, 403, 405}
        for path in probe_paths:
            for _ in range(2):
                try:
                    resp = await self._client.get(f"{self.base_url}{path}", headers=self._headers())
                    if resp.status_code in acceptable_status_codes:
                        return True
                except Exception:
                    continue
        return False

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
            raise BackendUnavailableError("mineru base_url is not configured")
        async with self._semaphore:
            opts = dict(options or {})
            parse_backend = str(opts.get("parse_backend", self.parse_backend))
            parse_method = str(opts.get("parse_method", self.parse_method))
            page_fallback_enabled = _parse_bool(opts.get("page_fallback_enabled"), default=True)
            has_page_fallback_target_chars = "page_fallback_target_chars" in opts or "target_chars" in opts
            has_page_fallback_max_pages = "page_fallback_max_pages" in opts
            page_fallback_target_chars = _parse_int(
                opts.get("page_fallback_target_chars", opts.get("target_chars", 3200)),
                default=3200,
                minimum=0,
            )
            page_fallback_max_pages = _parse_int(opts.get("page_fallback_max_pages", 24), default=24, minimum=1)

            base_payload = self._extract_request_payload(parse_backend, parse_method, opts)
            payload_candidates = self._adaptive_payloads(base_payload)
            errors: list[str] = []
            resource_limited_failure = False
            endpoint_unavailable: str | None = None

            await self._wait_for_capacity()

            for payload in payload_candidates:
                try:
                    response = await self._request_parse(
                        document=document,
                        filename=filename,
                        mime_type=mime_type,
                        payload=payload,
                    )
                    text = _coerce_mineru_text(response).strip()
                    if not text:
                        text = _decode_best_effort(document).strip()
                    return DocumentIR(
                        source_uri=source_uri,
                        provider_id=self.provider_id,
                        provider_version=self.provider_version,
                        text_blocks=_to_text_blocks(text) or [TextBlock(text=text)],
                        quality={"confidence": 0.9},
                        metadata={"backend": payload.get("backend"), "parse_method": payload.get("parse_method")},
                    )
                except InvalidRequestError as exc:
                    message = str(exc)
                    errors.append(message)
                    if _looks_like_not_found(message):
                        endpoint_unavailable = message
                        break
                    if _looks_like_busy_error(message):
                        resource_limited_failure = True
                        break
                    if _looks_like_oom(message) or _looks_like_timeout(message):
                        resource_limited_failure = True
                        break

            if endpoint_unavailable:
                raise InvalidRequestError(f"mineru endpoint unavailable (route not found): {endpoint_unavailable}")

            if page_fallback_enabled and _supports_page_fallback(filename, mime_type):
                if resource_limited_failure:
                    if not has_page_fallback_target_chars:
                        page_fallback_target_chars = 600
                    if not has_page_fallback_max_pages:
                        page_fallback_max_pages = min(page_fallback_max_pages, 3)
                return await self._parse_pages(
                    document=document,
                    filename=filename,
                    source_uri=source_uri,
                    mime_type=mime_type,
                    payload_candidates=payload_candidates,
                    target_chars=page_fallback_target_chars,
                    max_pages=page_fallback_max_pages,
                )

            if resource_limited_failure:
                detail = errors[-1] if errors else "resource-limited parser failure"
                raise InvalidRequestError(f"mineru parse failed due resource constraints: {detail}")
            detail = errors[-1] if errors else "unknown error"
            raise InvalidRequestError(f"mineru parse failed after adaptive retries: {detail}")
