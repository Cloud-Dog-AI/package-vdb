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

"""Stable identifier and hashing helpers (sections 5.4, 5.18, 8, 19.4).

All identifiers are deterministic functions of their inputs so that re-indexing
an unchanged workbook produces identical object keys and hashes, which is what
makes incremental refresh and stable-ID assertions possible.
"""

from __future__ import annotations

import hashlib
from typing import Any

_SEP = "\x1f"  # unit separator: unlikely to appear in spreadsheet identifiers


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def stable_id(*parts: Any) -> str:
    """Return a short, stable identifier derived from the given parts."""
    joined = _SEP.join("" if p is None else str(p) for p in parts)
    return _digest(joined)[:24]


def content_hash(text: str) -> str:
    """Return the full SHA-256 hex digest of ``text`` (section 5.18)."""
    return _digest(text)


def bytes_hash(data: bytes) -> str:
    """Return the full SHA-256 hex digest of raw bytes (file hash, section 5.2)."""
    return hashlib.sha256(data).hexdigest()


def schema_signature(column_names: list[str], type_hints: list[str]) -> str:
    """Return a stable signature of a table schema (section 5.4 schema signature).

    Used to detect header/shape changes across workbook versions independently
    of row content.
    """
    payload = _SEP.join(f"{name}:{hint}" for name, hint in zip(column_names, type_hints))
    return _digest(payload)[:32]


def content_signature(rows: list[list[Any]]) -> str:
    """Return a stable signature of table content (section 5.4 content signature)."""
    flat = _SEP.join(_SEP.join("" if c is None else str(c) for c in row) for row in rows)
    return _digest(flat)[:32]
