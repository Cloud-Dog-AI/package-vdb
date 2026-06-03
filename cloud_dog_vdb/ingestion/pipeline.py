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

import mimetypes
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse

from cloud_dog_vdb.domain.models import Record
from cloud_dog_vdb.ingestion.acquire import acquire_bytes, acquire_text
from cloud_dog_vdb.ingestion.chunk.base import Chunker
from cloud_dog_vdb.ingestion.chunk.recursive import RecursiveChunker
from cloud_dog_vdb.ingestion.ocr.planner import decide_ocr
from cloud_dog_vdb.ingestion.parse.base import ParserProvider
from cloud_dog_vdb.ingestion.parse.ir import DocumentIR
from cloud_dog_vdb.ingestion.parse.providers.deepdoc import DeepDocParserProvider
from cloud_dog_vdb.ingestion.parse.providers.docling import DoclingParserProvider
from cloud_dog_vdb.ingestion.parse.providers.internal import InternalParserProvider
from cloud_dog_vdb.ingestion.parse.providers.marker_mcp import MarkerMcpParserProvider
from cloud_dog_vdb.ingestion.parse.providers.mineru import MineruParserProvider
from cloud_dog_vdb.ingestion.parse.providers.transformers import TransformersParserProvider
from cloud_dog_vdb.ingestion.parse.registry import ParserRegistry
from cloud_dog_vdb.ingestion.table.policy import normalise_table_policy
from cloud_dog_vdb.ingestion.table.renderers import render_tables


CheckpointCallback = Callable[[str, int], None]


@dataclass(frozen=True, slots=True)
class ParserIngestionOptions:
    """Represent parser ingestion options."""

    parser_chain: list[str] = field(default_factory=lambda: ["internal"])
    parser_options: dict[str, dict[str, Any]] = field(default_factory=dict)
    quality: str = ""
    ocr_enabled: bool | None = None
    page_fallback_target_chars: int | None = None
    page_fallback_max_pages: int | None = None
    parser_provider: str = ""
    ocr_mode: str = "disabled"
    ocr_provider: str = ""
    ocr_min_chars: int = 200
    ocr_min_scanned_ratio: float = 0.5
    table_policy: str = "table_as_markdown"
    table_json_shape: str = "records"
    chunk_unit: str = "characters"
    chunk_size: int = 800
    chunk_overlap: int = 120
    min_quality_confidence: float = 0.0


def _effective_parser_chain(opts: ParserIngestionOptions) -> list[str]:
    chain = list(opts.parser_chain)
    if not chain:
        chain = ["internal"]
    parser_provider = str(opts.parser_provider or "").strip()
    if not parser_provider:
        return chain
    out = [parser_provider]
    out.extend(provider_id for provider_id in chain if provider_id != parser_provider)
    if parser_provider != "internal" and "internal" not in out:
        out.append("internal")
    return out


def _quality_preset_overrides(provider_id: str, quality: str) -> dict[str, Any]:
    preset = str(quality or "").strip().lower()
    if provider_id != "mineru" or not preset:
        return {}
    if preset == "fast":
        return {
            "parse_backend": "pipeline",
            "parse_method": "txt",
            "formula_enable": False,
            "table_enable": False,
            "return_middle_json": False,
            "return_images": False,
        }
    if preset == "balanced":
        return {
            "parse_backend": "pipeline",
            "parse_method": "auto",
            "formula_enable": False,
            "table_enable": False,
            "return_middle_json": False,
            "return_images": False,
        }
    if preset == "best":
        return {
            "parse_backend": "hybrid-auto-engine",
            "parse_method": "auto",
            "formula_enable": True,
            "table_enable": True,
        }
    return {}


def _effective_parser_options(opts: ParserIngestionOptions, chain: list[str]) -> dict[str, dict[str, Any]]:
    effective: dict[str, dict[str, Any]] = {
        provider_id: dict(provider_options)
        for provider_id, provider_options in opts.parser_options.items()
    }
    for provider_id in chain:
        if provider_id == "internal":
            continue
        provider_options = dict(effective.get(provider_id, {}))
        for key, value in _quality_preset_overrides(provider_id, opts.quality).items():
            provider_options.setdefault(key, value)
        if provider_id == "mineru":
            if opts.ocr_enabled is not None:
                provider_options["parse_method"] = "ocr" if opts.ocr_enabled else "txt"
            if opts.page_fallback_target_chars is not None:
                provider_options["page_fallback_target_chars"] = max(int(opts.page_fallback_target_chars), 0)
            if opts.page_fallback_max_pages is not None:
                provider_options["page_fallback_max_pages"] = max(int(opts.page_fallback_max_pages), 1)
        effective[provider_id] = provider_options
    return effective


def build_parser_registry(services: dict[str, dict[str, Any]] | None = None) -> ParserRegistry:
    """Build parser registry."""
    services = services or {}
    registry = ParserRegistry()
    registry.register(InternalParserProvider())

    mineru_cfg = services.get("mineru", {})
    if isinstance(mineru_cfg, dict) and str(mineru_cfg.get("base_url", "")).strip():
        registry.register(
            MineruParserProvider(
                base_url=str(mineru_cfg.get("base_url", "")),
                api_key=str(mineru_cfg.get("api_key", "")),
                timeout_seconds=float(mineru_cfg.get("timeout_seconds", 120.0) or 120.0),
                request_retries=int(mineru_cfg.get("request_retries", 3) or 3),
                busy_retry_attempts=int(mineru_cfg.get("busy_retry_attempts", 3) or 3),
                busy_retry_initial_seconds=float(mineru_cfg.get("busy_retry_initial_seconds", 5.0) or 5.0),
                busy_retry_max_delay_seconds=float(
                    mineru_cfg.get("busy_retry_max_delay_seconds", 20.0) or 20.0
                ),
                busy_retry_backoff=float(mineru_cfg.get("busy_retry_backoff", 2.0) or 2.0),
                health_path=str(mineru_cfg.get("health_path", "/health") or "/health"),
                health_timeout_seconds=float(mineru_cfg.get("health_timeout_seconds", 8.0) or 8.0),
                max_concurrent_requests=int(mineru_cfg.get("max_concurrent_requests", 1) or 1),
            )
        )

    marker_cfg = services.get("marker_mcp", {})
    if isinstance(marker_cfg, dict) and str(marker_cfg.get("base_url", "")).strip():
        registry.register(
            MarkerMcpParserProvider(
                base_url=str(marker_cfg.get("base_url", "")),
                auth_token=str(marker_cfg.get("auth_token", "")),
                timeout_seconds=float(marker_cfg.get("timeout_seconds", 120.0) or 120.0),
                request_retries=int(marker_cfg.get("request_retries", 3) or 3),
                async_threshold_seconds=float(marker_cfg.get("async_threshold_seconds", 30.0) or 30.0),
                async_poll_interval_seconds=float(marker_cfg.get("async_poll_interval_seconds", 5.0) or 5.0),
                async_max_wait_seconds=float(marker_cfg.get("async_max_wait_seconds", 600.0) or 600.0),
                async_status_path=str(marker_cfg.get("async_status_path", "")),
                async_result_path=str(marker_cfg.get("async_result_path", "")),
                async_cancel_path=str(marker_cfg.get("async_cancel_path", "")),
            )
        )

    deepdoc_cfg = services.get("deepdoc", {})
    if isinstance(deepdoc_cfg, dict) and deepdoc_cfg.get("enabled"):
        registry.register(
            DeepDocParserProvider(
                command=list(deepdoc_cfg.get("command", ["deepdoc"])),
                timeout_seconds=float(deepdoc_cfg.get("timeout_seconds", 120.0) or 120.0),
            )
        )

    docling_cfg = services.get("docling", {})
    if isinstance(docling_cfg, dict) and docling_cfg.get("enabled"):
        registry.register(
            DoclingParserProvider(
                command=list(docling_cfg.get("command", ["docling"])),
                timeout_seconds=float(docling_cfg.get("timeout_seconds", 120.0) or 120.0),
            )
        )

    transformers_cfg = services.get("transformers", {})
    if isinstance(transformers_cfg, dict) and transformers_cfg.get("enabled"):
        command = transformers_cfg.get("command")
        registry.register(
            TransformersParserProvider(
                base_url=str(transformers_cfg.get("base_url", "")),
                api_key=str(transformers_cfg.get("api_key", "")),
                endpoint_path=str(transformers_cfg.get("endpoint_path", "/parse")),
                command=list(command) if isinstance(command, list) else [],
                timeout_seconds=float(transformers_cfg.get("timeout_seconds", 120.0) or 120.0),
            )
        )
    return registry


def _now_utc() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _guess_source_type(source_uri: str) -> str:
    value = source_uri.lower()
    if value.startswith(("http://", "https://")):
        return "web"
    if value.startswith("file://"):
        return "file"
    if value.startswith("api://"):
        return "api"
    return "other"


def _filename_from_source_uri(source_uri: str) -> str:
    value = str(source_uri).strip()
    if not value:
        return ""
    parsed = urlparse(value)
    path = parsed.path if parsed.path else value
    filename = Path(unquote(path)).name.strip()
    return filename


def _render_ir(ir: DocumentIR, *, table_policy: str, table_json_shape: str) -> str:
    parts: list[str] = []
    base_text = ir.full_text().strip()
    if base_text:
        parts.append(base_text)
    tables = render_tables(ir.table_blocks, policy=table_policy, json_shape=table_json_shape)
    if tables:
        parts.extend(tables)
    return "\n\n".join(part for part in parts if part.strip()).strip()


async def _parse_with_chain(
    registry: ParserRegistry,
    chain: list[str],
    *,
    document: bytes,
    filename: str,
    source_uri: str,
    mime_type: str | None,
    parser_options: dict[str, dict[str, Any]] | None = None,
) -> tuple[DocumentIR, str, str]:
    parser_options = parser_options or {}
    errors: list[str] = []
    for provider_id in chain:
        provider: ParserProvider | None = registry.get(provider_id)
        if provider is None:
            errors.append(f"{provider_id}:not_registered")
            continue
        try:
            out = await provider.parse_bytes(
                document,
                filename=filename,
                source_uri=source_uri,
                mime_type=mime_type,
                options=parser_options.get(provider_id, {}),
            )
            return out, provider.provider_id, provider.provider_version
        except Exception as exc:
            errors.append(f"{provider_id}:{type(exc).__name__}:{exc}")
    raise RuntimeError("all parser providers failed: " + " | ".join(errors))


async def ingest_text(
    vdb_client, collection: str, source_text: str, chunker, converter, record_prefix: str = "r"
) -> list[str]:
    """Handle ingest text."""
    raw = acquire_text(source_text)
    converted = converter.convert(raw)
    chunks = chunker.chunk(converted)
    created_at = _now_utc()
    records = [
        Record(
            record_id=f"{record_prefix}-{i}",
            content=chunk,
            metadata={
                "tenant_id": "default",
                "source_uri": f"ingestion://{collection}/{record_prefix}/{i}",
                "source_type": "other",
                "lifecycle_state": "active",
                "created_at": created_at,
            },
        )
        for i, chunk in enumerate(chunks)
    ]
    return await vdb_client.upsert_records(collection, records)


async def ingest_document(
    vdb_client,
    collection: str,
    source: bytes | str,
    *,
    source_uri: str = "",
    record_prefix: str = "r",
    parser_registry: ParserRegistry | None = None,
    parser_services: dict[str, dict[str, Any]] | None = None,
    chunker: Chunker | None = None,
    options: ParserIngestionOptions | None = None,
    metadata: dict[str, Any] | None = None,
    on_checkpoint: CheckpointCallback | None = None,
) -> list[str]:
    """Handle ingest document."""
    opts = options or ParserIngestionOptions()
    table_policy = normalise_table_policy(opts.table_policy)
    checkpoint = on_checkpoint or (lambda stage, count: None)

    checkpoint("acquire", 0)
    raw_bytes, filename = acquire_bytes(source)
    resolved_source_uri = source_uri or f"file://{filename}"
    source_uri_filename = _filename_from_source_uri(resolved_source_uri)
    if filename in {"document.bin", "inline.txt"} and source_uri_filename:
        filename = source_uri_filename
    mime_type = mimetypes.guess_type(filename)[0]
    if not mime_type and source_uri_filename:
        mime_type = mimetypes.guess_type(source_uri_filename)[0]
    checkpoint("acquire", 1)

    checkpoint("parse", 0)
    registry = parser_registry or build_parser_registry(parser_services)
    effective_chain = _effective_parser_chain(opts)
    effective_parser_options = _effective_parser_options(opts, effective_chain)
    ir, parser_provider, parser_version = await _parse_with_chain(
        registry,
        effective_chain,
        document=raw_bytes,
        filename=filename,
        source_uri=resolved_source_uri,
        mime_type=mime_type,
        parser_options=effective_parser_options,
    )
    checkpoint("parse", 1)

    text = _render_ir(ir, table_policy=table_policy, table_json_shape=opts.table_json_shape)
    scanned_ratio = float(ir.quality.get("scanned_ratio", 0.0))
    decision = decide_ocr(
        mode=opts.ocr_mode,
        text_chars=len(text),
        scanned_ratio=scanned_ratio,
        provider_id=opts.ocr_provider,
        min_chars=opts.ocr_min_chars,
        min_scanned_ratio=opts.ocr_min_scanned_ratio,
    )

    checkpoint("chunk", 0)
    selected_chunker = chunker or RecursiveChunker(max_chars=max(opts.chunk_size, 32))
    chunks = selected_chunker.chunk_non_empty(text)
    checkpoint("chunk", len(chunks))

    base_metadata = dict(metadata or {})
    created_at = _now_utc()
    base_metadata.setdefault("tenant_id", "default")
    base_metadata.setdefault("source_uri", resolved_source_uri)
    base_metadata.setdefault("source_type", _guess_source_type(resolved_source_uri))
    base_metadata.setdefault("lifecycle_state", "active")
    base_metadata.setdefault("created_at", created_at)
    base_metadata["parser_provider"] = parser_provider
    base_metadata["parser_version"] = parser_version
    base_metadata["ocr_mode"] = decision.mode
    base_metadata["ocr_applied"] = decision.enabled
    base_metadata["ocr_reason"] = decision.reason
    base_metadata["table_policy"] = table_policy
    base_metadata["table_json_shape"] = opts.table_json_shape
    base_metadata["chunk_unit"] = opts.chunk_unit

    checkpoint("upsert", 0)
    records = [
        Record(
            record_id=f"{record_prefix}-{index}",
            content=chunk,
            metadata={**base_metadata, "chunk_id": str(index)},
        )
        for index, chunk in enumerate(chunks)
    ]
    out = await vdb_client.upsert_records(collection, records)
    checkpoint("upsert", len(out))
    return out


class IngestionPipeline:
    """Represent ingestion pipeline."""

    def __init__(
        self,
        *,
        vdb_client: Any,
        chunker: Chunker | None = None,
        parser_registry: ParserRegistry | None = None,
        parser_services: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.vdb_client = vdb_client
        self.chunker = chunker
        self.parser_registry = parser_registry
        self.parser_services = parser_services

    async def ingest(
        self,
        collection: str,
        source: bytes | str,
        *,
        source_uri: str = "",
        options: ParserIngestionOptions | None = None,
        metadata: dict[str, Any] | None = None,
        on_checkpoint: CheckpointCallback | None = None,
    ) -> list[str]:
        """Handle ingest."""
        return await ingest_document(
            self.vdb_client,
            collection,
            source,
            source_uri=source_uri,
            parser_registry=self.parser_registry,
            parser_services=self.parser_services,
            chunker=self.chunker,
            options=options,
            metadata=metadata,
            on_checkpoint=on_checkpoint,
        )
