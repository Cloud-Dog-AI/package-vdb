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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

import httpx

from cloud_dog_vdb.ingestion.chunk.recursive import RecursiveChunker
from cloud_dog_vdb.ingestion.parse.base import ParserProvider
from cloud_dog_vdb.ingestion.parse.ir import DocumentIR
from cloud_dog_vdb.ingestion.parse.providers.deepdoc import DeepDocParserProvider
from cloud_dog_vdb.ingestion.parse.providers.docling import DoclingParserProvider
from cloud_dog_vdb.ingestion.parse.providers.internal import InternalParserProvider
from cloud_dog_vdb.ingestion.parse.providers.marker_mcp import MarkerMcpParserProvider
from cloud_dog_vdb.ingestion.parse.providers.mineru import MineruParserProvider
from cloud_dog_vdb.ingestion.parse.providers.transformers import TransformersParserProvider


def _looks_like_heading(text: str) -> bool:
    stripped = text.strip()
    return stripped.startswith("#") and len(stripped) > 1


def _quality_invariant_pass(ir: DocumentIR, corpus_entry: dict[str, Any]) -> bool:
    expectations = corpus_entry.get("expectations")
    if not isinstance(expectations, dict):
        expectations = {}
    text = ir.full_text()
    if len(text.strip()) < int(expectations.get("min_text_chars", 1) or 1):
        return False
    if bool(expectations.get("require_headings", False)):
        if not any(block.kind == "heading" or _looks_like_heading(block.text) for block in ir.text_blocks):
            return False
    if bool(expectations.get("require_tables", False)):
        if len(ir.table_blocks) <= 0 and "|" not in text and "<table" not in text.lower():
            return False
    return True


def _chunk_count(ir: DocumentIR) -> int:
    text = ir.full_text().strip()
    if not text:
        return 0
    return len(RecursiveChunker(max_chars=800).chunk_non_empty(text))


@dataclass(frozen=True, slots=True)
class ComparisonContext:
    """Represent comparison context."""

    provider_id: str
    document_id: str
    filename: str
    category: str


def _cfg_float(cfg: dict[str, Any], key: str, default: float) -> float:
    raw = cfg.get(key, default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _provider_timeout_seconds(provider_id: str, cfg: dict[str, Any]) -> float:
    explicit = _cfg_float(cfg, "doc_timeout_seconds", 0.0)
    if explicit > 0:
        return max(explicit, 1.0)
    base = float(cfg.get("timeout_seconds", 120.0) or 120.0)
    multiplier = _cfg_float(cfg, "doc_timeout_multiplier", 2.5)
    minimum = _cfg_float(cfg, "doc_timeout_min_seconds", 90.0)
    maximum = _cfg_float(cfg, "doc_timeout_max_seconds", 900.0)
    computed = max(base * max(multiplier, 1.0), minimum)
    return min(computed, max(maximum, minimum))


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return False
    return text in {"1", "true", "yes", "on", "busy"}


async def _provider_busy_status(provider_id: str, cfg: dict[str, Any]) -> tuple[bool, str]:
    if provider_id != "marker_mcp":
        return False, ""
    base_url = str(cfg.get("base_url", "")).strip()
    if not base_url:
        return False, ""
    health_path = str(cfg.get("health_path", "/health")).strip() or "/health"
    if not health_path.startswith("/"):
        health_path = "/" + health_path
    timeout_seconds = max(_cfg_float(cfg, "health_timeout_seconds", 8.0), 1.0)
    url = f"{base_url.rstrip('/')}{health_path}"
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            resp = await client.get(url)
    except Exception as exc:
        return False, f"health_probe_error:{type(exc).__name__}:{exc}"
    if resp.status_code >= 400:
        return False, f"health_http_{resp.status_code}"
    try:
        payload = resp.json() if resp.content else {}
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        busy = _truthy(payload.get("busy"))
        status = str(payload.get("status", "")).strip().lower()
        if busy or status in {"busy", "degraded_busy", "overloaded"}:
            inflight = payload.get("inflight_requests")
            workers = payload.get("workers")
            return True, f"status={status},inflight={inflight},workers={workers}"
    return False, ""


class CrossProviderComparison:
    """Run the same documents across parser providers and capture comparable metrics."""

    def __init__(
        self,
        *,
        services: dict[str, dict[str, Any]] | None = None,
        provider_ids: list[str] | None = None,
    ) -> None:
        self.services = services or {}
        self.provider_ids = list(provider_ids or [])

    def _enabled_provider_ids(self) -> list[str]:
        if self.provider_ids:
            return list(self.provider_ids)
        order = ["internal", "marker_mcp", "mineru", "deepdoc", "docling", "transformers"]
        out: list[str] = []
        for provider_id in order:
            if provider_id == "internal":
                out.append(provider_id)
                continue
            cfg = self.services.get(provider_id, {})
            if isinstance(cfg, dict) and bool(cfg.get("enabled", False)):
                out.append(provider_id)
        return out

    def _build_provider(self, provider_id: str) -> ParserProvider:
        cfg = dict(self.services.get(provider_id, {}))
        if provider_id == "internal":
            return InternalParserProvider()
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
            )
        if provider_id == "mineru":
            base_url = str(cfg.get("base_url", "")).strip()
            if not base_url:
                raise ValueError("mineru enabled but base_url is missing")
            return MineruParserProvider(
                base_url=base_url,
                api_key=str(cfg.get("api_key", "")),
                timeout_seconds=float(cfg.get("timeout_seconds", 120.0) or 120.0),
                request_retries=int(cfg.get("request_retries", 3) or 3),
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
        raise ValueError(f"Unsupported provider_id: {provider_id}")

    async def compare_document(
        self,
        *,
        document: bytes,
        filename: str,
        source_uri: str,
        mime_type: str,
        corpus_entry: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Handle compare document."""
        cases: list[dict[str, Any]] = []
        document_id = str(corpus_entry.get("id", filename))
        category = str(corpus_entry.get("category", ""))
        providers = self._enabled_provider_ids()
        for provider_id in providers:
            context = ComparisonContext(
                provider_id=provider_id,
                document_id=document_id,
                filename=filename,
                category=category,
            )
            cfg = dict(self.services.get(provider_id, {}))
            try:
                provider = self._build_provider(provider_id)
            except Exception as exc:
                cases.append(
                    {
                        "provider_id": context.provider_id,
                        "document_id": context.document_id,
                        "filename": context.filename,
                        "category": context.category,
                        "status": "unavailable",
                        "reason": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue

            is_busy, busy_reason = await _provider_busy_status(provider_id, cfg)
            if is_busy:
                cases.append(
                    {
                        "provider_id": context.provider_id,
                        "document_id": context.document_id,
                        "filename": context.filename,
                        "category": context.category,
                        "status": "busy",
                        "reason": busy_reason or "provider_busy",
                    }
                )
                continue

            if not await provider.health_check():
                cases.append(
                    {
                        "provider_id": context.provider_id,
                        "document_id": context.document_id,
                        "filename": context.filename,
                        "category": context.category,
                        "status": "unavailable",
                        "reason": "health_check_false",
                    }
                )
                continue

            options: dict[str, Any] = {}
            if provider_id == "marker_mcp":
                options = {"output_format": "markdown", "paginate_output": True}
            elif provider_id == "mineru":
                options = {
                    "parse_backend": "pipeline",
                    "parse_method": "auto",
                    "formula_enable": False,
                    "table_enable": False,
                    "return_middle_json": False,
                    "return_images": False,
                    "page_fallback_enabled": True,
                    "page_fallback_max_pages": 24,
                    "page_fallback_target_chars": 3200,
                }

            started = perf_counter()
            timeout_seconds = _provider_timeout_seconds(provider_id, cfg)
            try:
                ir = await asyncio.wait_for(
                    provider.parse_bytes(
                        document,
                        filename=filename,
                        source_uri=source_uri,
                        mime_type=mime_type,
                        options=options,
                    ),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:
                cases.append(
                    {
                        "provider_id": context.provider_id,
                        "document_id": context.document_id,
                        "filename": context.filename,
                        "category": context.category,
                        "status": "error",
                        "reason": f"ParserTimeoutError: parse exceeded {timeout_seconds:.1f}s",
                        "parse_time_ms": (perf_counter() - started) * 1000.0,
                    }
                )
                continue
            except Exception as exc:
                cases.append(
                    {
                        "provider_id": context.provider_id,
                        "document_id": context.document_id,
                        "filename": context.filename,
                        "category": context.category,
                        "status": "error",
                        "reason": f"{type(exc).__name__}: {exc}",
                        "parse_time_ms": (perf_counter() - started) * 1000.0,
                    }
                )
                continue

            full_text = ir.full_text()
            heading_count = sum(
                1 for block in ir.text_blocks if block.kind == "heading" or _looks_like_heading(block.text)
            )
            execution_mode = str(ir.metadata.get("execution_mode", "sync"))
            cases.append(
                {
                    "provider_id": context.provider_id,
                    "document_id": context.document_id,
                    "filename": context.filename,
                    "category": context.category,
                    "status": "ok",
                    "execution_mode": execution_mode,
                    "parse_time_ms": (perf_counter() - started) * 1000.0,
                    "text_chars": len(full_text),
                    "heading_count": heading_count,
                    "table_count": len(ir.table_blocks),
                    "image_count": len(ir.artefact_refs),
                    "chunk_count": _chunk_count(ir),
                    "quality_invariant_pass_rate": 1.0 if _quality_invariant_pass(ir, corpus_entry) else 0.0,
                }
            )
        return cases

    async def compare_corpus(
        self,
        *,
        corpus_root: Path,
        corpus_entries: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Handle compare corpus."""
        results: list[dict[str, Any]] = []
        for entry in corpus_entries:
            file_name = str(entry.get("file", ""))
            source_path = corpus_root / file_name
            if not source_path.is_file():
                results.append(
                    {
                        "provider_id": "all",
                        "document_id": str(entry.get("id", file_name)),
                        "filename": file_name,
                        "category": str(entry.get("category", "")),
                        "status": "error",
                        "reason": f"missing_corpus_file:{source_path}",
                    }
                )
                continue
            results.extend(
                await self.compare_document(
                    document=source_path.read_bytes(),
                    filename=source_path.name,
                    source_uri=f"file://{source_path.name}",
                    mime_type="application/pdf",
                    corpus_entry=entry,
                )
            )

        providers = sorted({str(case.get("provider_id", "")) for case in results if str(case.get("provider_id", ""))})
        summary: dict[str, Any] = {}
        for provider_id in providers:
            provider_cases = [case for case in results if str(case.get("provider_id", "")) == provider_id]
            ok_cases = [case for case in provider_cases if case.get("status") == "ok"]
            summary[provider_id] = {
                "total": len(provider_cases),
                "ok": len(ok_cases),
                "success_ratio": (len(ok_cases) / len(provider_cases)) if provider_cases else 0.0,
                "mean_parse_time_ms": (
                    sum(float(case.get("parse_time_ms", 0.0) or 0.0) for case in ok_cases) / len(ok_cases)
                    if ok_cases
                    else 0.0
                ),
                "quality_invariant_pass_rate": (
                    sum(float(case.get("quality_invariant_pass_rate", 0.0) or 0.0) for case in ok_cases) / len(ok_cases)
                    if ok_cases
                    else 0.0
                ),
            }

        return {
            "generated_at": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "providers": providers,
            "cases": results,
            "summary": summary,
        }
