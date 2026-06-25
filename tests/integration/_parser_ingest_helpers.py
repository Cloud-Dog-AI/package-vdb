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

import uuid
from pathlib import Path
from typing import Any

import pytest

from cloud_dog_vdb import CollectionSpec, SearchRequest, get_vdb_client
from cloud_dog_vdb.ingestion.pipeline import ParserIngestionOptions, build_parser_registry, ingest_document


def _service_parser(services: dict[str, dict[str, object]], parser_id: str) -> dict[str, object]:
    cfg = services.get(parser_id, {})
    if not isinstance(cfg, dict):
        pytest.fail(f"{parser_id} parser service config missing", pytrace=False)
    if not cfg.get("enabled"):
        pytest.fail(f"{parser_id} parser provider is not enabled for IT2 parser-first ingestion tests", pytrace=False)
    if parser_id in {"mineru", "marker_mcp", "transformers"}:
        base_url = str(cfg.get("base_url", "")).strip()
        command = cfg.get("command")
        has_command = isinstance(command, list) and len(command) > 0
        if not base_url and not has_command:
            pytest.fail(
                f"{parser_id} parser provider requires base_url or command for IT2 parser-first ingestion tests",
                pytrace=False,
            )
    if parser_id in {"deepdoc", "docling"}:
        command = cfg.get("command")
        if not isinstance(command, list) or not command:
            pytest.fail(
                f"{parser_id} parser provider requires command for IT2 parser-first ingestion tests", pytrace=False
            )
    return cfg


def _build_backend_config(provider_id: str, cfg: dict[str, Any]) -> dict[str, Any]:
    out = {
        "enabled": True,
        "local_mode": False,
        "timeout_seconds": 60,
    }
    for key in (
        "base_url",
        "url",
        "api_key",
        "auth_token",
        "host",
        "port",
        "username",
        "password",
        "database",
        "database_uri",
        "tls",
        "ssl",
    ):
        if key in cfg and cfg.get(key) not in ("", None):
            out[key] = cfg.get(key)
    return out


def _corpus_path(corpus_entries: list[dict[str, str]]) -> Path:
    package_root = Path(__file__).resolve().parents[2]
    preferred_ids = (
        "item_cod_form",
        "z83_example",
        "sample_rural_completed_form",
        "fw9_form",
        "examples_pdf",
    )
    for preferred_id in preferred_ids:
        entry = next((item for item in corpus_entries if str(item.get("id", "")) == preferred_id), None)
        if entry is not None:
            return package_root / "test-data" / str(entry["file"])

    pdf_entry = next((entry for entry in corpus_entries if str(entry.get("file", "")).lower().endswith(".pdf")), None)
    if pdf_entry is not None:
        return package_root / "test-data" / str(pdf_entry["file"])

    return package_root / "test-data" / str(corpus_entries[0]["file"])


async def _required_provider_diagnostic(
    *,
    required_parser_provider: str,
    parser_services: dict[str, dict[str, Any]],
    parser_options: dict[str, dict[str, Any]],
    source_path: Path,
) -> str:
    if required_parser_provider == "internal":
        return "internal provider selected by test configuration"
    registry = build_parser_registry(parser_services)
    provider = registry.get(required_parser_provider)
    if provider is None:
        return f"{required_parser_provider} not registered in parser registry"
    try:
        ir = await provider.parse_bytes(
            source_path.read_bytes(),
            filename=source_path.name,
            source_uri=f"file://{source_path.name}",
            mime_type="application/pdf",
            options=parser_options.get(required_parser_provider, {}),
        )
        return (
            f"{required_parser_provider} direct parse succeeded: "
            f"text_chars={len(ir.full_text())}, provider_version={ir.provider_version}"
        )
    except Exception as exc:
        return f"{required_parser_provider} direct parse error: {type(exc).__name__}: {exc}"


async def run_parser_first_roundtrip(
    *,
    provider_id: str,
    backend_cfg: dict[str, Any],
    services: dict[str, dict[str, object]],
    corpus_entries: list[dict[str, str]],
    parser_chain: list[str] | None = None,
    parser_services_override: dict[str, dict[str, Any]] | None = None,
    parser_options_override: dict[str, dict[str, Any]] | None = None,
    required_parser_provider: str | None = None,
) -> tuple[list[str], list]:
    chain = list(parser_chain or ["mineru", "internal"])
    parser_options = dict(parser_options_override or {})
    parser_services = dict(parser_services_override or {})
    if not parser_services:
        for parser_id in chain:
            if parser_id == "internal":
                continue
            cfg = _service_parser(services, parser_id)
            resolved: dict[str, Any] = {}
            for key in ("enabled", "base_url", "api_key", "auth_token", "timeout_seconds", "command", "endpoint_path"):
                value = cfg.get(key)
                if value in ("", None):
                    continue
                if isinstance(value, list) and not value:
                    continue
                resolved[key] = value
            parser_services[parser_id] = resolved

    source_path = _corpus_path(corpus_entries)
    collection = f"cloud_dog_ai_it2_{provider_id}_{uuid.uuid4().hex[:8]}"
    runtime_config = {
        "vector_stores": {
            "default_backend": provider_id,
            provider_id: _build_backend_config(provider_id, backend_cfg),
        }
    }
    client = get_vdb_client(runtime_config)
    created = False
    try:
        await client.create_collection(CollectionSpec(name=collection, embedding_dim=4))
        created = True
        ids = await ingest_document(
            client,
            collection,
            source_path.read_bytes(),
            source_uri=f"file://{source_path.name}",
            options=ParserIngestionOptions(
                parser_chain=chain,
                parser_options=parser_options,
                ocr_mode="disabled",
                table_policy="table_as_markdown",
                chunk_size=700,
            ),
            parser_services=parser_services,
            metadata={"tenant_id": "it2", "source_type": "file"},
        )
        records = await client.list_records(collection, {"tenant_id": "it2"})
        assert len(ids) >= 1
        assert len(records) >= 1
        parser_provider = str(records[0].metadata.get("parser_provider", ""))
        if required_parser_provider:
            if parser_provider != required_parser_provider:
                diagnostic = await _required_provider_diagnostic(
                    required_parser_provider=required_parser_provider,
                    parser_services=parser_services,
                    parser_options=parser_options,
                    source_path=source_path,
                )
                assert parser_provider == required_parser_provider, (
                    f"expected parser_provider={required_parser_provider}, actual={parser_provider}; {diagnostic}"
                )
        else:
            assert parser_provider in set(chain)

        if provider_id == "opensearch":
            assert await client.count_documents(collection) >= 1
        else:
            search = await client.search(
                collection, SearchRequest(query_text="*", top_k=3, filters={"tenant_id": "it2"})
            )
            assert len(search.results) >= 1
        return ids, records
    finally:
        if created:
            try:
                await client.delete_collection(collection)
            except Exception:
                pass
