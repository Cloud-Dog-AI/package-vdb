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

# cloud_dog_vdb — Runtime factory
"""Build VDBClient runtime from resolved configuration data."""

from __future__ import annotations

from typing import Any

from cloud_dog_vdb.adapters.factory import build_adapter
from cloud_dog_vdb.adapters.registry import AdapterRegistry
from cloud_dog_vdb.config.models import ProviderConfig
from cloud_dog_vdb.runtime.client import VDBClient


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on"}


def provider_config_from_dict(provider_id: str, raw: dict[str, Any]) -> ProviderConfig:
    """Handle provider config from dict."""
    host = str(raw.get("host", ""))
    port = int(raw.get("port", 0) or 0)
    base_url = str(raw.get("base_url", raw.get("url", "")))
    if not base_url and host and port:
        scheme = "https" if _parse_bool(raw.get("tls", raw.get("ssl", False))) else "http"
        base_url = f"{scheme}://{host}:{port}"
    return ProviderConfig(
        provider_id=provider_id,
        base_url=base_url,
        api_key=str(raw.get("api_key", raw.get("auth_token", ""))),
        username=str(raw.get("username", "")),
        password=str(raw.get("password", "")),
        host=host,
        port=port,
        database=str(raw.get("database", "")),
        database_uri=str(raw.get("database_uri", "")),
        timeout_seconds=float(raw.get("timeout_seconds", 30.0) or 30.0),
    )


def build_runtime_client(config: dict[str, Any]) -> VDBClient:
    """Build runtime client."""
    registry = AdapterRegistry()
    stores = config.get("vector_stores", {})
    default_provider = str(stores.get("default_backend", ""))

    for provider_id in ("chroma", "qdrant", "weaviate", "opensearch", "pgvector", "infinity"):
        raw = stores.get(provider_id)
        if not isinstance(raw, dict) or not raw.get("enabled", False):
            continue
        provider_config = provider_config_from_dict(provider_id, raw)
        registry.register(provider_id, build_adapter(provider_config, local_mode=bool(raw.get("local_mode", False))))

    if not default_provider:
        ids = registry.list_ids()
        if not ids:
            raise ValueError("No enabled vector store backends configured")
        default_provider = ids[0]

    return VDBClient(registry, default_provider)
