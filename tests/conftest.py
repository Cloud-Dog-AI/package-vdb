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
import os
import shlex
import threading
import uuid
from pathlib import Path
from typing import Any, Callable

import httpx
import pytest


REQUIRED_VAULT_VARS = ["VAULT_ADDR", "VAULT_TOKEN", "VAULT_MOUNT_POINT", "VAULT_CONFIG_PATH"]
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
CORPUS_MANIFEST_PATH = PACKAGE_ROOT / "test-data" / "corpus-manifest.yaml"
ENV_TIER_TOKENS = {"UT", "ST", "IT", "AT", "QT", "PT", "CT"}
TIER_TEST_DIRS = {
    "UT": {"unit", "quality"},
    "ST": {"system"},
    "IT": {"integration"},
    "AT": {"application"},
    "QT": {"security"},
    "PT": {"parser", "parser_performance"},
    "CT": {"compatibility"},
}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--env",
        action="append",
        default=None,
        help="Required env file path(s). Can be repeated or comma-separated.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Restrict collection to the test tier selected via TEST_ENV_TIER."""
    tier = str(os.environ.get("TEST_ENV_TIER", "")).strip().upper()
    if not tier:
        env_files = _normalise_env_files(config.getoption("--env"))
        for path in env_files:
            if not Path(path).is_file():
                continue
            tier = str(_load_env_file(path).get("TEST_ENV_TIER", "")).strip().upper()
            if tier:
                break
    allowed_dirs = TIER_TEST_DIRS.get(tier)
    if not allowed_dirs:
        return

    selected: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        parts = Path(str(item.fspath)).parts
        top_level = ""
        for index, part in enumerate(parts):
            if part == "tests" and index + 1 < len(parts):
                top_level = parts[index + 1]
                break
        if top_level in allowed_dirs:
            selected.append(item)
        else:
            deselected.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected


def _normalise_env_files(raw: list[str] | None) -> list[str]:
    out: list[str] = []
    tests_dir = Path(__file__).resolve().parent
    for value in raw or []:
        for part in value.split(","):
            p = part.strip()
            if p:
                token = p.upper()
                if token in ENV_TIER_TOKENS:
                    tier_file = tests_dir / f"env-{token}"
                    if tier_file.is_file():
                        out.append(str(tier_file))
                else:
                    out.append(p)
    return out


def _load_env_file(path: str) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            raise pytest.UsageError(f"Invalid env line in {path}: {line}")
        k, v = s.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _test_env_tier() -> str:
    return str(os.environ.get("TEST_ENV_TIER", "")).strip().upper()


def _tier_requires_vault() -> bool:
    return _test_env_tier() in {"IT", "AT"}


def _run_coroutine_in_thread(factory: Callable[[], Any]) -> Any:
    """Run async probe work in an isolated event loop thread-safe for async test sessions."""

    output: dict[str, Any] = {}
    errors: dict[str, BaseException] = {}

    def _worker() -> None:
        try:
            output["value"] = asyncio.run(factory())
        except BaseException as exc:  # pragma: no cover - defensive passthrough
            errors["exc"] = exc

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    thread.join()

    if "exc" in errors:
        raise errors["exc"]
    return output.get("value")


def _require_backend_config(vdbs: dict, provider_id: str) -> dict[str, Any]:
    cfg = vdbs.get(provider_id, {})
    if not isinstance(cfg, dict) or not cfg:
        pytest.fail(f"dev.vdbs.{provider_id} missing from Vault config", pytrace=False)
    return cfg


def _probe_name(provider_id: str) -> str:
    # Use backend-safe names: avoid leading underscores and keep simple lowercase tokens.
    return f"clouddogwriteprobe{provider_id}{uuid.uuid4().hex[:10]}"


def _clean_scalar(value: str) -> str:
    return value.strip().strip('"').strip("'")


def _parse_bool(raw: str, default: bool = False) -> bool:
    if raw is None:
        return default
    text = str(raw).strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def _parse_int(raw: str, default: int) -> int:
    text = str(raw).strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        return default


def _parse_scalar(raw: str) -> Any:
    text = _clean_scalar(raw)
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    if text.lower() in {"null", "none"}:
        return None
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        pass
    return text


def _parse_command(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    return shlex.split(text)


def _parse_corpus_manifest(path: Path) -> dict[str, Any]:
    """Parse the stable manifest structure without external YAML dependencies."""
    text = path.read_text(encoding="utf-8")
    entries: list[dict[str, Any]] = []
    current_entry: dict[str, Any] | None = None

    section = ""
    subsection = ""
    in_expectations = False

    performance_metrics: list[str] = []
    performance_thresholds: dict[str, Any] = {}
    provider_sets: dict[str, list[str]] = {"parser": [], "ocr": [], "embedding": []}

    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        stripped = raw_line.strip()

        if stripped in {"corpus:", "performance_baselines:", "providers:"}:
            if current_entry and section == "corpus":
                entries.append(current_entry)
                current_entry = None
            section = stripped[:-1]
            subsection = ""
            in_expectations = False
            continue

        if section == "corpus":
            if stripped.startswith("- id:"):
                if current_entry:
                    entries.append(current_entry)
                current_entry = {"id": _clean_scalar(stripped.split(":", 1)[1]), "expectations": {}}
                in_expectations = False
                continue
            if current_entry is None:
                continue
            if stripped == "expectations:":
                in_expectations = True
                continue
            if ":" not in stripped:
                continue
            key, value = [part.strip() for part in stripped.split(":", 1)]
            if in_expectations:
                current_entry.setdefault("expectations", {})[key] = _parse_scalar(value)
            else:
                current_entry[key] = _parse_scalar(value)
            continue

        if section == "performance_baselines":
            if stripped in {"metrics:", "thresholds:"}:
                subsection = stripped[:-1]
                continue
            if stripped.startswith("- ") and subsection == "metrics":
                performance_metrics.append(str(_parse_scalar(stripped[2:])))
                continue
            if ":" in stripped and subsection == "thresholds":
                key, value = [part.strip() for part in stripped.split(":", 1)]
                performance_thresholds[key] = _parse_scalar(value)
            continue

        if section == "providers":
            if stripped in {"parser:", "ocr:", "embedding:"}:
                subsection = stripped[:-1]
                continue
            if stripped.startswith("- ") and subsection in provider_sets:
                provider_sets[subsection].append(str(_parse_scalar(stripped[2:])))
            continue

    if current_entry and section == "corpus":
        entries.append(current_entry)

    return {
        "path": str(path),
        "entries": entries,
        "ids": [str(entry.get("id", "")) for entry in entries],
        "files": [str(entry.get("file", "")) for entry in entries],
        "performance_baselines": {
            "metrics": performance_metrics,
            "thresholds": performance_thresholds,
        },
        "providers": provider_sets,
    }


def _service_cfg_from_env(service_id: str) -> dict[str, Any]:
    if service_id == "mineru":
        return {
            "base_url": str(os.environ.get("MINERU_BASE_URL", "")),
            "api_key": str(os.environ.get("MINERU_API_KEY", "")),
            "timeout_seconds": float(os.environ.get("MINERU_TIMEOUT_SECONDS", "30") or 30),
            "request_retries": _parse_int(os.environ.get("MINERU_REQUEST_RETRIES", "3"), 3),
            "busy_retry_attempts": _parse_int(os.environ.get("MINERU_BUSY_RETRY_ATTEMPTS", "3"), 3),
            "busy_retry_initial_seconds": float(os.environ.get("MINERU_BUSY_RETRY_INITIAL_SECONDS", "5") or 5),
            "busy_retry_max_delay_seconds": float(
                os.environ.get("MINERU_BUSY_RETRY_MAX_DELAY_SECONDS", "20") or 20
            ),
            "busy_retry_backoff": float(os.environ.get("MINERU_BUSY_RETRY_BACKOFF", "2") or 2),
            "health_path": str(os.environ.get("MINERU_HEALTH_PATH", "/health")),
            "health_timeout_seconds": float(os.environ.get("MINERU_HEALTH_TIMEOUT_SECONDS", "8") or 8),
            "max_concurrent_requests": _parse_int(os.environ.get("MINERU_MAX_CONCURRENT_REQUESTS", "1"), 1),
            "enabled": _parse_bool(os.environ.get("MINERU_ENABLED", ""), default=False),
        }
    if service_id == "marker_mcp":
        return {
            "base_url": str(os.environ.get("MARKER_MCP_BASE_URL", "")),
            "auth_token": str(os.environ.get("MARKER_MCP_AUTH_TOKEN", "")),
            "timeout_seconds": float(os.environ.get("MARKER_MCP_TIMEOUT_SECONDS", "30") or 30),
            "request_retries": _parse_int(os.environ.get("MARKER_MCP_REQUEST_RETRIES", "3"), 3),
            "async_threshold_seconds": float(os.environ.get("MARKER_MCP_ASYNC_THRESHOLD_SECONDS", "30") or 30),
            "async_poll_interval_seconds": float(os.environ.get("MARKER_MCP_ASYNC_POLL_INTERVAL_SECONDS", "5") or 5),
            "async_max_wait_seconds": float(os.environ.get("MARKER_MCP_ASYNC_MAX_WAIT_SECONDS", "600") or 600),
            "async_status_path": str(os.environ.get("MARKER_MCP_ASYNC_STATUS_PATH", "")),
            "async_result_path": str(os.environ.get("MARKER_MCP_ASYNC_RESULT_PATH", "")),
            "async_cancel_path": str(os.environ.get("MARKER_MCP_ASYNC_CANCEL_PATH", "")),
            "busy_retry_initial_seconds": float(os.environ.get("MARKER_MCP_BUSY_RETRY_INITIAL_SECONDS", "35") or 35),
            "busy_retry_max_seconds": float(os.environ.get("MARKER_MCP_BUSY_RETRY_MAX_SECONDS", "420") or 420),
            "busy_retry_max_delay_seconds": float(
                os.environ.get("MARKER_MCP_BUSY_RETRY_MAX_DELAY_SECONDS", "60") or 60
            ),
            "busy_retry_backoff": float(os.environ.get("MARKER_MCP_BUSY_RETRY_BACKOFF", "1.5") or 1.5),
            "enabled": _parse_bool(os.environ.get("MARKER_MCP_ENABLED", ""), default=False),
        }
    if service_id == "deepdoc":
        command = _parse_command(os.environ.get("DEEPDOC_COMMAND", ""))
        return {
            "command": command,
            "timeout_seconds": float(os.environ.get("DEEPDOC_TIMEOUT_SECONDS", "120") or 120),
            "enabled": _parse_bool(os.environ.get("DEEPDOC_ENABLED", ""), default=bool(command)),
        }
    if service_id == "docling":
        command = _parse_command(os.environ.get("DOCLING_COMMAND", ""))
        return {
            "command": command,
            "timeout_seconds": float(os.environ.get("DOCLING_TIMEOUT_SECONDS", "120") or 120),
            "enabled": _parse_bool(os.environ.get("DOCLING_ENABLED", ""), default=bool(command)),
        }
    if service_id == "transformers":
        command = _parse_command(os.environ.get("TRANSFORMERS_COMMAND", ""))
        base_url = str(os.environ.get("TRANSFORMERS_BASE_URL", ""))
        return {
            "base_url": base_url,
            "api_key": str(os.environ.get("TRANSFORMERS_API_KEY", "")),
            "endpoint_path": str(os.environ.get("TRANSFORMERS_ENDPOINT_PATH", "/parse")),
            "command": command,
            "timeout_seconds": float(os.environ.get("TRANSFORMERS_TIMEOUT_SECONDS", "120") or 120),
            "enabled": _parse_bool(
                os.environ.get("TRANSFORMERS_ENABLED", ""),
                default=bool(base_url.strip() or command),
            ),
        }
    return {}


def _merge_service_cfg(vault_cfg: dict[str, Any], env_cfg: dict[str, Any]) -> dict[str, Any]:
    merged = dict(vault_cfg or {})
    for key, value in env_cfg.items():
        if isinstance(value, bool):
            merged[key] = value
            continue
        if value in ("", 0, 0.0, None):
            continue
        if isinstance(value, list) and not value:
            continue
        merged[key] = value
    if not merged.get("base_url") and merged.get("uri"):
        merged["base_url"] = merged.get("uri")
    if "enabled" not in merged:
        merged["enabled"] = bool(merged.get("base_url") or merged.get("command"))
    return merged


def _pick_service_cfg(vault_services: dict[str, Any], aliases: list[str]) -> dict[str, Any]:
    for name in aliases:
        value = vault_services.get(name)
        if isinstance(value, dict) and value:
            return value
    return {}


def _optional_vault_dev_config() -> dict[str, Any]:
    if any(not os.environ.get(key) for key in REQUIRED_VAULT_VARS):
        return {}
    url = (
        f"{os.environ['VAULT_ADDR'].rstrip('/')}/v1/"
        f"{os.environ['VAULT_MOUNT_POINT']}/data/{os.environ['VAULT_CONFIG_PATH']}"
    )
    try:
        resp = httpx.get(url, headers={"X-Vault-Token": os.environ["VAULT_TOKEN"]}, timeout=20.0)
        if resp.status_code >= 400:
            return {}
        data = resp.json().get("data", {}).get("data", {}).get("json", {})
        dev = data.get("dev", {}) if isinstance(data, dict) else {}
        return dev if isinstance(dev, dict) else {}
    except Exception:
        return {}


@pytest.fixture(scope="session", autouse=True)
def _enforce_and_load_env(request: pytest.FixtureRequest) -> None:
    env_files = _normalise_env_files(request.config.getoption("--env"))
    if not env_files:
        raise pytest.UsageError("Missing required --env <file>.")
    locked = set(os.environ.keys())
    merged: dict[str, str] = {}
    for path in env_files:
        if not Path(path).is_file():
            raise pytest.UsageError(f"Env file not found: {path}")
        for k, v in _load_env_file(path).items():
            if k in locked:
                continue
            merged[k] = v
    os.environ.update(merged)


@pytest.fixture(scope="session")
def vault_config() -> dict:
    missing = [k for k in REQUIRED_VAULT_VARS if not os.environ.get(k)]
    if missing:
        if _tier_requires_vault():
            pytest.fail(
                "VAULT_TOKEN and Vault variables are REQUIRED for "
                f"{_test_env_tier()} tests. Missing: {', '.join(missing)}. "
                "Run: set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a",
                pytrace=False,
            )
        pytest.skip(f"Vault variables missing (UT/ST may skip): {', '.join(missing)}")

    url = (
        f"{os.environ['VAULT_ADDR'].rstrip('/')}/v1/"
        f"{os.environ['VAULT_MOUNT_POINT']}/data/{os.environ['VAULT_CONFIG_PATH']}"
    )
    try:
        resp = httpx.get(url, headers={"X-Vault-Token": os.environ["VAULT_TOKEN"]}, timeout=20.0)
    except httpx.HTTPError as exc:
        message = f"Vault config request failed: {exc}"
        if _tier_requires_vault():
            pytest.fail(message, pytrace=False)
        pytest.skip(message)
    if resp.status_code >= 400:
        message = f"Vault config not reachable: {resp.status_code}"
        if _tier_requires_vault():
            pytest.fail(message, pytrace=False)
        pytest.skip(message)

    data = resp.json().get("data", {}).get("data", {}).get("json", {})
    return data.get("dev", {}) if isinstance(data, dict) else {}


@pytest.fixture(scope="session")
def vdbs(vault_config: dict) -> dict:
    out = vault_config.get("vdbs", {})
    if not out:
        message = "dev.vdbs not found in vault config"
        if _tier_requires_vault():
            pytest.fail(message, pytrace=False)
        pytest.skip(message)
    return out


@pytest.fixture(scope="session")
def models(vault_config: dict) -> dict:
    out = vault_config.get("models", {})
    if not out:
        message = "dev.models not found in vault config"
        if _tier_requires_vault():
            pytest.fail(message, pytrace=False)
        pytest.skip(message)
    return out


@pytest.fixture(scope="session")
def chroma_ready(vdbs: dict) -> dict:
    cfg = _require_backend_config(vdbs, "chroma")

    from cloud_dog_vdb.adapters.chroma import ChromaAdapter
    from cloud_dog_vdb.domain.models import CollectionSpec
    from cloud_dog_vdb.runtime.factory import provider_config_from_dict

    async def _probe() -> None:
        adapter = ChromaAdapter(provider_config_from_dict("chroma", cfg), local_mode=False)
        probe = _probe_name("chroma")
        try:
            await adapter.delete_collection(probe)
            await adapter.create_collection(CollectionSpec(name=probe, embedding_dim=4))
            await adapter.add_documents(probe, ["probe"], [{"tenant_id": "probe"}], ["probe-1"])
        finally:
            try:
                await adapter.delete_collection(probe)
            except Exception:
                pass
            await adapter._client.aclose()

    try:
        _run_coroutine_in_thread(_probe)
    except Exception as exc:
        pytest.fail(f"Chroma write-path probe FAILED: {exc}", pytrace=False)
    return cfg


@pytest.fixture(scope="session")
def qdrant_ready(vdbs: dict) -> dict:
    cfg = _require_backend_config(vdbs, "qdrant")

    from cloud_dog_vdb.adapters.qdrant import QdrantAdapter
    from cloud_dog_vdb.domain.models import CollectionSpec
    from cloud_dog_vdb.runtime.factory import provider_config_from_dict

    async def _probe() -> None:
        adapter = QdrantAdapter(provider_config_from_dict("qdrant", cfg), local_mode=False)
        probe = _probe_name("qdrant")
        try:
            await adapter.delete_collection(probe)
            await adapter.create_collection(CollectionSpec(name=probe, embedding_dim=4))
            await adapter.add_documents(probe, ["probe"], [{"tenant_id": "probe"}], ["probe-1"])
        finally:
            try:
                await adapter.delete_collection(probe)
            except Exception:
                pass
            await adapter._client.aclose()

    try:
        _run_coroutine_in_thread(_probe)
    except Exception as exc:
        pytest.fail(f"Qdrant write-path probe FAILED: {exc}", pytrace=False)
    return cfg


@pytest.fixture(scope="session")
def weaviate_ready(vdbs: dict) -> dict:
    cfg = _require_backend_config(vdbs, "weaviate")

    from cloud_dog_vdb.adapters.weaviate import WeaviateAdapter
    from cloud_dog_vdb.domain.models import CollectionSpec
    from cloud_dog_vdb.runtime.factory import provider_config_from_dict

    async def _probe() -> None:
        adapter = WeaviateAdapter(provider_config_from_dict("weaviate", cfg), local_mode=False)
        probe = _probe_name("weaviate")
        try:
            await adapter.delete_collection(probe)
            await adapter.create_collection(CollectionSpec(name=probe, embedding_dim=4))
            await adapter.add_documents(probe, ["probe"], [{"tenant_id": "probe"}], ["probe-1"])
        finally:
            try:
                await adapter.delete_collection(probe)
            except Exception:
                pass
            await adapter._client.aclose()

    try:
        _run_coroutine_in_thread(_probe)
    except Exception as exc:
        pytest.fail(f"Weaviate write-path probe FAILED: {exc}", pytrace=False)
    return cfg


@pytest.fixture(scope="session")
def opensearch_ready(vdbs: dict) -> dict:
    cfg = _require_backend_config(vdbs, "opensearch")

    from cloud_dog_vdb.adapters.opensearch import OpenSearchAdapter
    from cloud_dog_vdb.domain.models import CollectionSpec
    from cloud_dog_vdb.runtime.factory import provider_config_from_dict

    async def _probe() -> None:
        adapter = OpenSearchAdapter(provider_config_from_dict("opensearch", cfg), local_mode=False)
        probe = _probe_name("opensearch")
        try:
            await adapter.delete_collection(probe)
            await adapter.create_collection(CollectionSpec(name=probe, embedding_dim=4))
            await adapter.add_documents(probe, ["probe"], [{"tenant_id": "probe"}], ["probe-1"])
        finally:
            try:
                await adapter.delete_collection(probe)
            except Exception:
                pass
            await adapter._client.aclose()

    try:
        _run_coroutine_in_thread(_probe)
    except Exception as exc:
        pytest.fail(f"OpenSearch write-path probe FAILED: {exc}", pytrace=False)
    return cfg


@pytest.fixture(scope="session")
def pgvector_ready(vdbs: dict) -> dict:
    cfg = _require_backend_config(vdbs, "pgvector")

    from cloud_dog_vdb.adapters.pgvector import PGVectorAdapter
    from cloud_dog_vdb.domain.models import CollectionSpec
    from cloud_dog_vdb.runtime.factory import provider_config_from_dict

    async def _probe() -> None:
        adapter = PGVectorAdapter(provider_config_from_dict("pgvector", cfg), local_mode=False)
        probe = _probe_name("pgvector")
        try:
            await adapter.delete_collection(probe)
            await adapter.create_collection(CollectionSpec(name=probe, embedding_dim=4))
            await adapter.add_documents(probe, ["probe"], [{"tenant_id": "probe"}], ["probe-1"])
        finally:
            try:
                await adapter.delete_collection(probe)
            except Exception:
                pass

    try:
        _run_coroutine_in_thread(_probe)
    except Exception as exc:
        pytest.fail(f"PGVector write-path probe FAILED: {exc}", pytrace=False)
    return cfg


@pytest.fixture(scope="session")
def corpus_manifest_path() -> Path:
    return CORPUS_MANIFEST_PATH


@pytest.fixture(scope="session")
def corpus_manifest(corpus_manifest_path: Path) -> dict[str, Any]:
    if not corpus_manifest_path.is_file():
        pytest.fail(f"Corpus manifest not found: {corpus_manifest_path}", pytrace=False)
    return _parse_corpus_manifest(corpus_manifest_path)


@pytest.fixture(scope="session")
def corpus_entries(corpus_manifest: dict[str, Any]) -> list[dict[str, str]]:
    entries = corpus_manifest.get("entries", [])
    if not entries:
        pytest.fail("Corpus manifest contains zero entries", pytrace=False)
    include_ids_raw = str(os.environ.get("CORPUS_INCLUDE_IDS", "")).strip()
    if not include_ids_raw:
        return entries
    include_ids = [item.strip() for item in include_ids_raw.split(",") if item.strip()]
    if not include_ids:
        return entries
    filtered = [entry for entry in entries if str(entry.get("id", "")) in set(include_ids)]
    missing = [item for item in include_ids if item not in {str(entry.get("id", "")) for entry in filtered}]
    if missing:
        pytest.fail(
            "CORPUS_INCLUDE_IDS contains unknown IDs: " + ", ".join(sorted(missing)),
            pytrace=False,
        )
    if not filtered:
        pytest.fail("CORPUS_INCLUDE_IDS filtering removed all corpus entries", pytrace=False)
    return filtered


@pytest.fixture(scope="session")
def services() -> dict[str, dict[str, Any]]:
    vault_services = _optional_vault_dev_config().get("services", {})
    mineru = _merge_service_cfg(_pick_service_cfg(vault_services, ["mineru"]), _service_cfg_from_env("mineru"))
    marker_mcp = _merge_service_cfg(
        _pick_service_cfg(vault_services, ["marker_mcp", "markermcp", "marker-mcp", "marker"]),
        _service_cfg_from_env("marker_mcp"),
    )
    deepdoc = _merge_service_cfg(_pick_service_cfg(vault_services, ["deepdoc"]), _service_cfg_from_env("deepdoc"))
    docling = _merge_service_cfg(_pick_service_cfg(vault_services, ["docling"]), _service_cfg_from_env("docling"))
    transformers = _merge_service_cfg(
        _pick_service_cfg(vault_services, ["transformers", "transformer_parser", "pdf_transformers"]),
        _service_cfg_from_env("transformers"),
    )
    return {
        "mineru": mineru,
        "marker_mcp": marker_mcp,
        "deepdoc": deepdoc,
        "docling": docling,
        "transformers": transformers,
    }


@pytest.fixture(scope="session")
def infinity_config(vdbs: dict) -> dict[str, Any]:
    cfg = vdbs.get("infinity", {})
    if isinstance(cfg, dict) and cfg:
        return cfg
    if _parse_bool(os.environ.get("ENABLE_INFINITY_TESTS", "")):
        pytest.fail(
            "Infinity tests explicitly enabled but dev.vdbs.infinity missing from Vault/env configuration",
            pytrace=False,
        )
    pytest.skip("Infinity backend not configured; set ENABLE_INFINITY_TESTS=1 with dev.vdbs.infinity to enforce")
