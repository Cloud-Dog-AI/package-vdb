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
import json
import math
import os
import re
import statistics
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

import httpx

from cloud_dog_vdb.ingestion.parse.base import ParserProvider
from cloud_dog_vdb.ingestion.parse.providers.deepdoc import DeepDocParserProvider
from cloud_dog_vdb.ingestion.parse.providers.docling import DoclingParserProvider
from cloud_dog_vdb.ingestion.parse.providers.marker_mcp import MarkerMcpParserProvider
from cloud_dog_vdb.ingestion.parse.providers.mineru import MineruParserProvider
from cloud_dog_vdb.ingestion.parse.providers.transformers import TransformersParserProvider


PROVIDER_ORDER = ("mineru", "marker_mcp", "deepdoc", "docling", "transformers")
_CACHE: dict[str, dict[str, Any]] = {}


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_text(name: str, default: str) -> str:
    value = str(os.environ.get(name, "")).strip()
    return value or default


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_bool_env(name: str, *, default: bool) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _provider_env_prefix(provider_id: str) -> str:
    return provider_id.upper().replace("-", "_")


def _provider_doc_timeout_seconds(provider_id: str, services: dict[str, dict[str, Any]]) -> float:
    prefix = _provider_env_prefix(provider_id)
    explicit = _env_float(f"{prefix}_DOC_TIMEOUT_SECONDS", 0.0)
    if explicit > 0:
        return max(explicit, 1.0)

    base_timeout = float(services.get(provider_id, {}).get("timeout_seconds", 120.0) or 120.0)
    multiplier = _env_float("PARSER_DOC_TIMEOUT_MULTIPLIER", 2.5)
    minimum = _env_float("PARSER_DOC_TIMEOUT_MIN_SECONDS", 90.0)
    maximum = _env_float("PARSER_DOC_TIMEOUT_MAX_SECONDS", 900.0)
    computed = max(base_timeout * max(multiplier, 1.0), minimum)
    return min(computed, max(maximum, minimum))


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return False
    return text in {"1", "true", "yes", "on", "busy"}


def _busy_status_from_payload(payload: dict[str, Any]) -> bool:
    if _is_truthy(payload.get("busy")):
        return True
    status = str(payload.get("status", "")).strip().lower()
    return status in {"busy", "degraded_busy", "overloaded"}


def _mineru_busy_status(payload: dict[str, Any]) -> bool:
    status = str(payload.get("status", "")).strip().lower()
    if status in {"overloaded", "degraded_busy"}:
        return True
    try:
        inflight = max(int(payload.get("inflight", 0) or 0), 0)
    except (TypeError, ValueError):
        inflight = 0
    try:
        waiting = max(int(payload.get("waiting", 0) or 0), 0)
    except (TypeError, ValueError):
        waiting = 0
    try:
        max_concurrent = max(int(payload.get("max_concurrent", 0) or 0), 0)
    except (TypeError, ValueError):
        max_concurrent = 0
    try:
        queue_max = max(int(payload.get("queue_max", 0) or 0), 0)
    except (TypeError, ValueError):
        queue_max = 0
    queue_has_capacity = queue_max > 0 and waiting < queue_max
    return waiting > 0 or (max_concurrent > 0 and inflight >= max_concurrent and not queue_has_capacity)


def _marker_error_is_busy(error_text: str) -> bool:
    busy_markers = (
        "worker busy",
        "queue wait exceeded",
        "provider busy",
        "too many requests",
        "http_429",
        "busy retries exhausted",
    )
    text = str(error_text or "").strip().lower()
    return bool(text) and any(marker in text for marker in busy_markers)


def _marker_error_is_timeout(error_text: str) -> bool:
    timeout_markers = (
        "parsertimeouterror",
        "timed out",
        "timeout budget",
        "parse exceeded",
    )
    text = str(error_text or "").strip().lower()
    return bool(text) and any(marker in text for marker in timeout_markers)


def _marker_error_is_saturated(error_text: str) -> bool:
    return _marker_error_is_busy(error_text) or _marker_error_is_timeout(error_text)


def _mineru_error_is_busy(error_text: str) -> bool:
    busy_markers = (
        "busy retries exhausted",
        "worker busy",
        "provider busy",
        "too many requests",
        "http_429",
        "http 429",
        "http 503",
        "readerror",
        "connecterror",
        "connecttimeout",
        "readtimeout",
        "remoteprotocolerror",
        "server disconnected without sending a response",
    )
    text = str(error_text or "").strip().lower()
    return bool(text) and any(marker in text for marker in busy_markers)


def _mineru_error_is_timeout(error_text: str) -> bool:
    timeout_markers = (
        "parsertimeouterror",
        "timed out",
        "parse exceeded",
    )
    text = str(error_text or "").strip().lower()
    return bool(text) and any(marker in text for marker in timeout_markers)


def _mineru_error_is_saturated(error_text: str) -> bool:
    return _mineru_error_is_busy(error_text) or _mineru_error_is_timeout(error_text)


async def _provider_runtime_state(provider_id: str, services: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if provider_id not in {"marker_mcp", "mineru"}:
        return {"state": "ready", "reason": "no_runtime_probe"}

    cfg = services.get(provider_id, {})
    base_url = str(cfg.get("base_url", "")).strip()
    if not base_url:
        return {"state": "unavailable", "reason": "missing_base_url"}

    health_path_var = "MARKER_MCP_HEALTH_PATH" if provider_id == "marker_mcp" else "MINERU_HEALTH_PATH"
    health_timeout_var = (
        "MARKER_MCP_HEALTH_TIMEOUT_SECONDS" if provider_id == "marker_mcp" else "MINERU_HEALTH_TIMEOUT_SECONDS"
    )
    health_path = _env_text(health_path_var, "/health")
    if not health_path.startswith("/"):
        health_path = "/" + health_path
    timeout_seconds = _env_float(health_timeout_var, 8.0)
    url = f"{base_url.rstrip('/')}{health_path}"
    try:
        async with httpx.AsyncClient(timeout=max(timeout_seconds, 1.0)) as client:
            resp = await client.get(url)
    except Exception as exc:
        return {"state": "unavailable", "reason": f"{type(exc).__name__}: {exc}", "url": url}

    if resp.status_code >= 500:
        return {"state": "unavailable", "reason": f"http_{resp.status_code}", "url": url}
    if resp.status_code >= 400:
        return {"state": "ready", "reason": f"health_http_{resp.status_code}", "url": url}

    try:
        payload = resp.json() if resp.content else {}
    except Exception:
        payload = {}

    if isinstance(payload, dict):
        if provider_id == "marker_mcp" and _busy_status_from_payload(payload):
            inflight = payload.get("inflight_requests")
            workers = payload.get("workers")
            detail = f"inflight={inflight}, workers={workers}, status={payload.get('status', '')}"
            return {"state": "busy", "reason": detail, "url": url}
        if provider_id == "mineru" and _mineru_busy_status(payload):
            detail = (
                f"inflight={payload.get('inflight')}, waiting={payload.get('waiting')}, "
                f"max_concurrent={payload.get('max_concurrent')}, queue_max={payload.get('queue_max')}, "
                f"status={payload.get('status', '')}"
            )
            return {"state": "busy", "reason": detail, "url": url}
    return {"state": "ready", "reason": "health_ok", "url": url}


def _build_soft_fail_summary(
    *,
    provider_id: str,
    corpus_entries: list[dict[str, Any]],
    runtime_state: dict[str, Any],
    reason: str,
) -> dict[str, Any]:
    expected_categories = sorted({str(entry.get("category", "")) for entry in corpus_entries})
    total = len(corpus_entries)
    return {
        "provider_id": provider_id,
        "generated_at": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "total_docs": total,
        "categories_covered": expected_categories,
        "success_count": 0,
        "success_ratio": 0.0,
        "expectation_pass_count": 0,
        "quality_invariant_pass_rate": 0.0,
        "average_completeness_ratio": 0.0,
        "parse_latency_p50_ms": 0.0,
        "parse_latency_p95_ms": 0.0,
        "throughput_pages_per_second": 0.0,
        "ocr_auto_trigger_ratio": 0.0,
        "average_confidence": 0.0,
        "cases": [],
        "runtime_state": runtime_state,
        "soft_fail_reason": reason,
        "soft_fail_detail": str(runtime_state.get("reason", "")).strip(),
    }


def _provider_parse_options(provider_id: str) -> dict[str, Any]:
    if provider_id == "mineru":
        return {
            "parse_backend": _env_text("MINERU_PARSE_BACKEND", "hybrid-auto-engine"),
            "parse_method": _env_text("MINERU_PARSE_METHOD", "auto"),
            # Use the lowest-VRAM request profile first for stability on shared GPU hosts.
            "formula_enable": _parse_bool_env("MINERU_FORMULA_ENABLE", default=False),
            "table_enable": _parse_bool_env("MINERU_TABLE_ENABLE", default=False),
            "return_middle_json": _parse_bool_env("MINERU_RETURN_MIDDLE_JSON", default=False),
            "return_images": _parse_bool_env("MINERU_RETURN_IMAGES", default=False),
            "page_fallback_enabled": True,
            "page_fallback_max_pages": max(_env_int("MINERU_PAGE_FALLBACK_MAX_PAGES", 24), 1),
        }
    if provider_id == "marker_mcp":
        # The marker-mcp server defaults to page_range="0" (first page only).
        # Override with MARKER_MCP_PAGE_RANGE to process enough pages for quality checks.
        page_range = _env_text("MARKER_MCP_PAGE_RANGE", "")
        if page_range:
            return {"page_range": page_range}
        return {}
    return {}


async def _wait_for_provider_ready(provider_id: str, services: dict[str, dict[str, Any]]) -> dict[str, Any]:
    state = await _provider_runtime_state(provider_id, services)
    if provider_id != "mineru" or state.get("state") != "busy":
        return state

    max_wait_seconds = _env_float("MINERU_BUSY_PROBE_MAX_WAIT_SECONDS", 60.0)
    poll_seconds = _env_float("MINERU_BUSY_PROBE_POLL_INTERVAL_SECONDS", 5.0)
    elapsed = 0.0
    while state.get("state") == "busy" and elapsed < max_wait_seconds:
        wait_for = min(poll_seconds, max_wait_seconds - elapsed)
        if wait_for <= 0:
            break
        await asyncio.sleep(wait_for)
        elapsed += wait_for
        state = await _provider_runtime_state(provider_id, services)
    if elapsed > 0:
        state = {**state, "wait_elapsed_seconds": elapsed}
    return state


def _percentile_ms(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = int(math.ceil((percentile / 100.0) * len(ordered))) - 1
    rank = max(0, min(rank, len(ordered) - 1))
    return float(ordered[rank])


def _looks_like_heading(text: str) -> bool:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    sample = lines[:250]
    for line in sample:
        if line.startswith("#"):
            return True
        if re.match(r"^\d+(\.\d+)*\s+[A-Z][A-Za-z0-9 ]+$", line):
            return True
        if len(line.split()) >= 3 and line.upper() == line and any(ch.isalpha() for ch in line):
            return True
    return False


def _looks_like_table(text: str) -> bool:
    lowered = text.lower()
    if "<table" in lowered and "<tr" in lowered:
        return True
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    for line in lines[:500]:
        if line.count("|") >= 2:
            return True
        if re.search(r"\S+\s{2,}\S+\s{2,}\S+", line):
            return True
        if len(re.findall(r"\d[\d,]*(?:\.\d+)?", line)) >= 3 and len(line.split()) >= 4:
            return True
    return False


def _entry_expectations(entry: dict[str, Any]) -> dict[str, Any]:
    raw = entry.get("expectations")
    if isinstance(raw, dict):
        return raw
    return {}


def _min_chars_for_pass(entry: dict[str, Any]) -> int:
    expectations = _entry_expectations(entry)
    min_chars = int(expectations.get("min_text_chars", 1) or 1)
    floor_ratio = _env_float("PARSER_EXPECTATION_TEXT_FLOOR_RATIO", 0.35)
    floor_ratio = min(max(floor_ratio, 0.05), 1.0)
    return max(64, int(min_chars * floor_ratio))


def enabled_parser_ids(services: dict[str, dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for provider_id in PROVIDER_ORDER:
        cfg = services.get(provider_id, {})
        if isinstance(cfg, dict) and bool(cfg.get("enabled")):
            out.append(provider_id)
    return out


def build_provider(provider_id: str, services: dict[str, dict[str, Any]]) -> ParserProvider:
    cfg = dict(services.get(provider_id, {}))
    if provider_id == "mineru":
        base_url = str(cfg.get("base_url", "")).strip()
        if not base_url:
            raise ValueError("mineru enabled but base_url is missing")
        return MineruParserProvider(
            base_url=base_url,
            api_key=str(cfg.get("api_key", "")),
            timeout_seconds=float(cfg.get("timeout_seconds", 120.0) or 120.0),
            request_retries=int(cfg.get("request_retries", 3) or 3),
            busy_retry_attempts=int(cfg.get("busy_retry_attempts", 3) or 3),
            busy_retry_initial_seconds=float(cfg.get("busy_retry_initial_seconds", 5.0) or 5.0),
            busy_retry_max_delay_seconds=float(cfg.get("busy_retry_max_delay_seconds", 20.0) or 20.0),
            busy_retry_backoff=float(cfg.get("busy_retry_backoff", 2.0) or 2.0),
            health_path=str(cfg.get("health_path", "/health") or "/health"),
            health_timeout_seconds=float(cfg.get("health_timeout_seconds", 8.0) or 8.0),
            max_concurrent_requests=int(cfg.get("max_concurrent_requests", 1) or 1),
        )
    if provider_id == "marker_mcp":
        base_url = str(cfg.get("base_url", "")).strip()
        if not base_url:
            raise ValueError("marker_mcp enabled but base_url is missing")
        return MarkerMcpParserProvider(
            base_url=base_url,
            auth_token=str(cfg.get("auth_token", "")),
            timeout_seconds=float(cfg.get("timeout_seconds", 120.0) or 120.0),
            request_retries=int(cfg.get("request_retries", 3) or 3),
            async_threshold_seconds=float(cfg.get("async_threshold_seconds", 30.0) or 30.0),
            async_poll_interval_seconds=float(cfg.get("async_poll_interval_seconds", 5.0) or 5.0),
            async_max_wait_seconds=float(cfg.get("async_max_wait_seconds", 600.0) or 600.0),
            async_status_path=str(cfg.get("async_status_path", "")),
            async_result_path=str(cfg.get("async_result_path", "")),
            async_cancel_path=str(cfg.get("async_cancel_path", "")),
            busy_retry_initial_seconds=float(cfg.get("busy_retry_initial_seconds", 35.0) or 35.0),
            busy_retry_max_seconds=float(cfg.get("busy_retry_max_seconds", 420.0) or 420.0),
            busy_retry_max_delay_seconds=float(cfg.get("busy_retry_max_delay_seconds", 60.0) or 60.0),
            busy_retry_backoff=float(cfg.get("busy_retry_backoff", 1.5) or 1.5),
        )
    if provider_id == "deepdoc":
        command = cfg.get("command", ["deepdoc"])
        if not isinstance(command, list) or not command:
            raise ValueError("deepdoc enabled but command is missing")
        return DeepDocParserProvider(
            command=list(command),
            timeout_seconds=float(cfg.get("timeout_seconds", 120.0) or 120.0),
        )
    if provider_id == "docling":
        command = cfg.get("command", ["docling"])
        if not isinstance(command, list) or not command:
            raise ValueError("docling enabled but command is missing")
        return DoclingParserProvider(
            command=list(command),
            timeout_seconds=float(cfg.get("timeout_seconds", 120.0) or 120.0),
        )
    if provider_id == "transformers":
        command = cfg.get("command", [])
        if not isinstance(command, list):
            command = []
        return TransformersParserProvider(
            base_url=str(cfg.get("base_url", "")),
            api_key=str(cfg.get("api_key", "")),
            endpoint_path=str(cfg.get("endpoint_path", "/parse")),
            command=command,
            timeout_seconds=float(cfg.get("timeout_seconds", 120.0) or 120.0),
        )
    raise ValueError(f"Unknown provider: {provider_id}")


def _report_path(provider_id: str) -> Path:
    package_root = Path(__file__).resolve().parents[2]
    report_dir = package_root / "tests" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir / f"parser_quality_matrix_{provider_id}.json"


async def evaluate_provider(
    *,
    provider_id: str,
    services: dict[str, dict[str, Any]],
    corpus_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    if provider_id in _CACHE:
        return _CACHE[provider_id]

    runtime_state = await _wait_for_provider_ready(provider_id, services)
    if runtime_state.get("state") == "busy":
        summary = _build_soft_fail_summary(
            provider_id=provider_id,
            corpus_entries=corpus_entries,
            runtime_state=runtime_state,
            reason="provider_busy",
        )
        _report_path(provider_id).write_text(json.dumps(summary, indent=2), encoding="utf-8")
        _CACHE[provider_id] = summary
        return summary

    provider = build_provider(provider_id, services)
    doc_timeout_seconds = _provider_doc_timeout_seconds(provider_id, services)
    provider_options = _provider_parse_options(provider_id)
    healthy = await provider.health_check()
    if not healthy:
        summary = _build_soft_fail_summary(
            provider_id=provider_id,
            corpus_entries=corpus_entries,
            runtime_state=runtime_state,
            reason="provider_unhealthy",
        )
        _report_path(provider_id).write_text(json.dumps(summary, indent=2), encoding="utf-8")
        _CACHE[provider_id] = summary
        return summary

    package_root = Path(__file__).resolve().parents[2]
    corpus_root = package_root / "test-data"
    cases: list[dict[str, Any]] = []

    for entry in corpus_entries:
        source_path = corpus_root / str(entry.get("file", ""))
        if not source_path.is_file():
            raise FileNotFoundError(f"Missing corpus file for parser benchmark: {source_path}")
        source_uri = f"file://{source_path.name}"
        case_id = str(entry.get("id", ""))
        print(
            f"[parser-matrix] provider={provider_id} start case={case_id} file={source_path.name} "
            f"doc_timeout={doc_timeout_seconds:.1f}s",
            flush=True,
        )
        start = perf_counter()
        error = ""
        text = ""
        headings = False
        tables = False
        confidence = 0.0
        success = False
        expectations = _entry_expectations(entry)
        min_chars_floor = _min_chars_for_pass(entry)

        try:
            request_options = dict(provider_options)
            if provider_id == "mineru":
                request_options["page_fallback_target_chars"] = max(
                    min_chars_floor,
                    _env_int("MINERU_PAGE_FALLBACK_TARGET_CHARS", 3200),
                )
            ir = await asyncio.wait_for(
                provider.parse_bytes(
                    source_path.read_bytes(),
                    filename=source_path.name,
                    source_uri=source_uri,
                    mime_type="application/pdf",
                    options=request_options,
                ),
                timeout=doc_timeout_seconds,
            )
            text = ir.full_text()
            confidence = float(ir.quality.get("confidence", 0.0) or 0.0)
            headings = _looks_like_heading(text)
            tables = bool(ir.table_blocks) or _looks_like_table(text)
            success = len(text.strip()) > 0
        except asyncio.TimeoutError:
            error = f"ParserTimeoutError: parse exceeded {doc_timeout_seconds:.1f}s"
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

        latency_ms = (perf_counter() - start) * 1000.0
        text_chars = len(text)
        words = len(text.split())
        require_headings = bool(expectations.get("require_headings", False))
        require_tables = bool(expectations.get("require_tables", False))
        min_chars_ok = text_chars >= min_chars_floor

        quality_checks = [min_chars_ok]
        if require_headings:
            quality_checks.append(headings)
        if require_tables:
            quality_checks.append(tables)
        expectation_pass = all(quality_checks)
        min_chars_base = int(expectations.get("min_text_chars", 1) or 1)
        completeness = min(float(text_chars) / float(max(min_chars_base, 1)), 1.0)

        cases.append(
            {
                "provider_id": provider_id,
                "id": case_id,
                "file": str(entry.get("file", "")),
                "category": str(entry.get("category", "")),
                "recommended_ocr_mode": str(entry.get("recommended_ocr_mode", "")),
                "success": success,
                "error": error,
                "latency_ms": latency_ms,
                "text_chars": text_chars,
                "word_count": words,
                "headings_detected": headings,
                "tables_detected": tables,
                "confidence": confidence,
                "expectation_pass": expectation_pass,
                "completeness_ratio": completeness,
            }
        )
        print(
            f"[parser-matrix] provider={provider_id} done case={case_id} success={success} "
            f"latency_ms={latency_ms:.1f} error={error or '-'}",
            flush=True,
        )
        if provider_id == "marker_mcp":
            saturated_cases = sum(1 for case in cases if _marker_error_is_saturated(str(case.get("error", ""))))
            processed = len(cases)
            # Early-exit when marker is clearly saturated: avoid burning the entire corpus budget.
            if processed >= 2 and saturated_cases == processed:
                summary = _build_soft_fail_summary(
                    provider_id=provider_id,
                    corpus_entries=corpus_entries,
                    runtime_state=runtime_state,
                    reason="provider_busy",
                )
                summary["cases"] = list(cases)
                summary["soft_fail_detail"] = (
                    f"early saturation: {saturated_cases}/{processed} cases returned busy/timeout markers"
                )
                _report_path(provider_id).write_text(json.dumps(summary, indent=2), encoding="utf-8")
                _CACHE[provider_id] = summary
                return summary
        if provider_id == "mineru":
            saturated_cases = sum(1 for case in cases if _mineru_error_is_saturated(str(case.get("error", ""))))
            processed = len(cases)
            if processed >= 2 and saturated_cases == processed:
                summary = _build_soft_fail_summary(
                    provider_id=provider_id,
                    corpus_entries=corpus_entries,
                    runtime_state=runtime_state,
                    reason="provider_busy",
                )
                summary["cases"] = list(cases)
                summary["soft_fail_detail"] = (
                    f"early saturation: {saturated_cases}/{processed} cases returned busy/timeout markers"
                )
                _report_path(provider_id).write_text(json.dumps(summary, indent=2), encoding="utf-8")
                _CACHE[provider_id] = summary
                return summary

    latencies = [float(case["latency_ms"]) for case in cases]
    successes = [case for case in cases if case["success"]]
    expectation_passes = [case for case in cases if case["expectation_pass"]]
    auto_cases = [case for case in cases if case["recommended_ocr_mode"] == "auto"]
    auto_low_text = [case for case in auto_cases if int(case["text_chars"]) < 200]
    total = len(cases)

    summary = {
        "provider_id": provider_id,
        "generated_at": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "total_docs": total,
        "categories_covered": sorted({str(case["category"]) for case in cases}),
        "success_count": len(successes),
        "success_ratio": (len(successes) / total) if total else 0.0,
        "expectation_pass_count": len(expectation_passes),
        "quality_invariant_pass_rate": (len(expectation_passes) / total) if total else 0.0,
        "average_completeness_ratio": (
            statistics.mean(float(case["completeness_ratio"]) for case in cases) if cases else 0.0
        ),
        "parse_latency_p50_ms": _percentile_ms(latencies, 50.0),
        "parse_latency_p95_ms": _percentile_ms(latencies, 95.0),
        "throughput_pages_per_second": (len(successes) / max(sum(latencies) / 1000.0, 1e-9)),
        "ocr_auto_trigger_ratio": (len(auto_low_text) / len(auto_cases)) if auto_cases else 0.0,
        "average_confidence": (statistics.mean(float(case["confidence"]) for case in cases) if cases else 0.0),
        "cases": cases,
        "runtime_state": runtime_state,
        "soft_fail_reason": "",
        "soft_fail_detail": "",
        "doc_timeout_seconds": doc_timeout_seconds,
    }
    print(
        f"[parser-matrix] provider={provider_id} summary success_ratio={summary['success_ratio']:.3f} "
        f"quality_pass_rate={summary['quality_invariant_pass_rate']:.3f} total={total}",
        flush=True,
    )
    if provider_id == "marker_mcp" and total:
        busy_case_count = 0
        saturated_case_count = 0
        for case in cases:
            error_text = str(case.get("error", "")).strip().lower()
            if not error_text:
                continue
            is_busy = _marker_error_is_busy(error_text)
            is_timeout = _marker_error_is_timeout(error_text)
            if is_busy:
                busy_case_count += 1
            if is_busy or is_timeout:
                saturated_case_count += 1
        if saturated_case_count == total and (busy_case_count > 0 or runtime_state.get("state") == "busy"):
            summary["soft_fail_reason"] = "provider_busy"
            summary["soft_fail_detail"] = (
                f"marker saturated: busy_cases={busy_case_count}/{total}, "
                f"saturated_cases={saturated_case_count}/{total}, "
                f"runtime_state={runtime_state.get('state', 'unknown')}"
            )
    if provider_id == "mineru" and total:
        busy_case_count = 0
        timeout_case_count = 0
        saturated_case_count = 0
        for case in cases:
            error_text = str(case.get("error", "")).strip().lower()
            if not error_text:
                continue
            is_busy = _mineru_error_is_busy(error_text)
            is_timeout = _mineru_error_is_timeout(error_text)
            if is_busy:
                busy_case_count += 1
            if is_timeout:
                timeout_case_count += 1
            if is_busy or is_timeout:
                saturated_case_count += 1
        if saturated_case_count == total and (
            busy_case_count > 0 or timeout_case_count == total or runtime_state.get("state") == "busy"
        ):
            summary["soft_fail_reason"] = "provider_busy"
            summary["soft_fail_detail"] = (
                f"mineru saturated: busy_cases={busy_case_count}/{total}, "
                f"timeout_cases={timeout_case_count}/{total}, "
                f"saturated_cases={saturated_case_count}/{total}, "
                f"runtime_state={runtime_state.get('state', 'unknown')}"
            )
    _report_path(provider_id).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _CACHE[provider_id] = summary
    return summary


async def evaluate_enabled_providers(
    *,
    services: dict[str, dict[str, Any]],
    corpus_entries: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for provider_id in enabled_parser_ids(services):
        out[provider_id] = await evaluate_provider(
            provider_id=provider_id,
            services=services,
            corpus_entries=corpus_entries,
        )
    return out
