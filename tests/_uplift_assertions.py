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

from pathlib import Path
from typing import Any


def assert_corpus_entry_shape(entry: dict[str, Any]) -> None:
    assert isinstance(entry.get("id"), str) and entry["id"].strip()
    assert isinstance(entry.get("file"), str) and entry["file"].strip()
    assert isinstance(entry.get("category", ""), str)
    assert isinstance(entry.get("recommended_ocr_mode", ""), str)


def assert_manifest_files_exist(package_root: Path, entries: list[dict[str, Any]]) -> None:
    data_dir = package_root / "test-data"
    for entry in entries:
        assert (data_dir / str(entry["file"])).is_file(), f"missing corpus file: {entry['file']}"


def build_min_runtime_config(provider_id: str) -> dict[str, Any]:
    providers = {
        "chroma": {"enabled": provider_id == "chroma", "local_mode": True},
        "qdrant": {"enabled": provider_id == "qdrant", "local_mode": True},
        "weaviate": {"enabled": provider_id == "weaviate", "local_mode": True},
        "opensearch": {"enabled": provider_id == "opensearch", "local_mode": True},
        "pgvector": {"enabled": provider_id == "pgvector", "local_mode": True},
        "infinity": {"enabled": provider_id == "infinity", "local_mode": True},
    }
    return {
        "vector_stores": {
            "default_backend": provider_id,
            **providers,
        }
    }


def service_has_base_url(service_cfg: dict[str, Any]) -> bool:
    return bool(str(service_cfg.get("base_url", "")).strip())
