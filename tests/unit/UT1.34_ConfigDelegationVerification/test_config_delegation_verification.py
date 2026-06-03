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


ROOT = Path(__file__).resolve().parents[3] / "cloud_dog_vdb"


FORBIDDEN = (
    "os.environ",
    "import hvac",
    "overlay_secrets",
    "_vault_from_env",
    "VAULT_JSON",
    "cloud_dog_vdb.secrets",
)


def test_no_local_secret_resolution_primitives_in_package() -> None:
    violations: list[str] = []
    for path in ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        if "testing" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN:
            if token in text:
                violations.append(f"{path.relative_to(ROOT)} -> {token}")
    assert violations == []


def test_all_adapters_use_provider_config_directly() -> None:
    adapter_dir = ROOT / "adapters"
    expected_tokens = {
        "chroma.py": ("self.config.api_key", "self.config.base_url"),
        "qdrant.py": ("self.config.api_key", "self.config.base_url"),
        "weaviate.py": ("self.config.api_key", "self.config.base_url"),
        "opensearch.py": ("self.config.username", "self.config.password"),
        "pgvector.py": ("self.config.database_uri",),
    }

    missing: list[str] = []
    for filename, tokens in expected_tokens.items():
        text = (adapter_dir / filename).read_text(encoding="utf-8")
        for token in tokens:
            if token not in text:
                missing.append(f"{filename}: missing {token}")
    assert missing == []
