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


def acquire_text(source: str) -> str:
    """Acquire text from inline content or filesystem path."""
    path = Path(source)
    if path.exists() and path.is_file():
        return path.read_text(encoding="utf-8")
    return source


def acquire_bytes(source: bytes | str) -> tuple[bytes, str]:
    """Handle acquire bytes."""
    if isinstance(source, bytes):
        return source, "document.bin"
    path = Path(source)
    if path.exists() and path.is_file():
        return path.read_bytes(), path.name
    return source.encode("utf-8"), "inline.txt"
