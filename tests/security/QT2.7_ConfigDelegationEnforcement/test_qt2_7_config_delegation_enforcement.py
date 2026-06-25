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


def test_qt2_7_no_hvac_imports_in_package() -> None:
    root = Path(__file__).resolve().parents[3] / "cloud_dog_vdb"
    python_files = [p for p in root.rglob("*.py") if "__pycache__" not in str(p)]
    offenders = [p for p in python_files if "import hvac" in p.read_text(encoding="utf-8")]
    assert offenders == []
