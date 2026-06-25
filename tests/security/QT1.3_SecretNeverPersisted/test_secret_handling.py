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


def test_no_secret_resolver_module_exists() -> None:
    root = Path(__file__).resolve().parents[3] / "cloud_dog_vdb"
    assert not (root / "secrets").exists()


def test_no_secret_keywords_in_logging_helpers() -> None:
    target = (Path(__file__).resolve().parents[3] / "cloud_dog_vdb" / "observability" / "audit.py").read_text(
        encoding="utf-8"
    )
    assert "api_key" not in target
    assert "password" not in target
