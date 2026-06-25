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

import os
from pathlib import Path
from typing import Any

from cloud_dog_vdb.testing.comparison import CrossProviderComparison
from cloud_dog_vdb.testing.comparison_report import write_comparison_report

_CACHE: dict[str, tuple[dict[str, Any], dict[str, Path]]] = {}


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _enrich_services_with_comparison_overrides(services: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    enriched: dict[str, dict[str, Any]] = {provider_id: dict(cfg) for provider_id, cfg in services.items()}
    global_multiplier = _env_float("PARSER_DOC_TIMEOUT_MULTIPLIER", 2.5)
    global_minimum = _env_float("PARSER_DOC_TIMEOUT_MIN_SECONDS", 90.0)
    global_maximum = _env_float("PARSER_DOC_TIMEOUT_MAX_SECONDS", 900.0)

    for provider_id in ("marker_mcp", "mineru", "deepdoc", "docling", "transformers", "internal"):
        provider_cfg = dict(enriched.get(provider_id, {}))
        prefix = provider_id.upper().replace("-", "_")
        explicit_timeout = _env_float(f"{prefix}_DOC_TIMEOUT_SECONDS", 0.0)
        if explicit_timeout > 0:
            provider_cfg["doc_timeout_seconds"] = explicit_timeout
        provider_cfg["doc_timeout_multiplier"] = global_multiplier
        provider_cfg["doc_timeout_min_seconds"] = global_minimum
        provider_cfg["doc_timeout_max_seconds"] = global_maximum

        if provider_id == "marker_mcp":
            provider_cfg["health_path"] = str(os.environ.get("MARKER_MCP_HEALTH_PATH", "/health")).strip() or "/health"
            provider_cfg["health_timeout_seconds"] = _env_float("MARKER_MCP_HEALTH_TIMEOUT_SECONDS", 8.0)

        enriched[provider_id] = provider_cfg

    return enriched


def _selected_provider_ids(services: dict[str, dict[str, Any]]) -> list[str]:
    configured = str(os.environ.get("COMPARISON_PROVIDERS", "")).strip()
    if configured:
        raw_ids = [item.strip() for item in configured.split(",") if item.strip()]
    else:
        raw_ids = []

    if not raw_ids:
        raw_ids = ["internal"] + [
            provider_id
            for provider_id in ("marker_mcp", "mineru", "deepdoc", "docling", "transformers")
            if bool(services.get(provider_id, {}).get("enabled", False))
        ]

    out: list[str] = []
    for provider_id in raw_ids:
        if provider_id == "internal":
            out.append(provider_id)
            continue
        if bool(services.get(provider_id, {}).get("enabled", False)):
            out.append(provider_id)
    if "internal" not in out:
        out.insert(0, "internal")
    return out


def _comparison_output_dir(package_root: Path) -> Path:
    raw = str(os.environ.get("COMPARISON_OUTPUT_DIR", "tests/comparison_reports")).strip()
    value = Path(raw)
    if value.is_absolute():
        return value
    return package_root / value


async def run_comparison(
    *,
    services: dict[str, dict[str, Any]],
    corpus_entries: list[dict[str, Any]],
    report_basename: str,
) -> tuple[dict[str, Any], dict[str, Path]]:
    package_root = Path(__file__).resolve().parents[2]
    effective_services = _enrich_services_with_comparison_overrides(services)
    provider_ids = _selected_provider_ids(effective_services)
    cache_key = "|".join(
        [
            ",".join(provider_ids),
            ",".join(str(entry.get("id", "")) for entry in corpus_entries),
        ]
    )
    output_dir = _comparison_output_dir(package_root)
    if cache_key in _CACHE:
        report, _ = _CACHE[cache_key]
        paths = write_comparison_report(
            report,
            output_dir=output_dir,
            report_basename=report_basename,
        )
        return report, paths

    comparison = CrossProviderComparison(services=effective_services, provider_ids=provider_ids)
    report = await comparison.compare_corpus(corpus_root=package_root / "test-data", corpus_entries=corpus_entries)
    paths = write_comparison_report(
        report,
        output_dir=output_dir,
        report_basename=report_basename,
    )
    _CACHE[cache_key] = (report, paths)
    return report, paths
