#!/usr/bin/env python3
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

"""Text enrichment helpers for local parser command adapters."""

from __future__ import annotations

import re
from typing import Any, Iterable


def _flatten_outline(nodes: Any) -> Iterable[Any]:
    if isinstance(nodes, list):
        for item in nodes:
            yield from _flatten_outline(item)
        return
    yield nodes


def outline_headings_from_reader(reader: Any, *, max_items: int = 80) -> list[str]:
    try:
        outline = reader.outline
    except Exception:
        return []

    lines: list[str] = []
    seen: set[str] = set()
    for node in _flatten_outline(outline):
        title = getattr(node, "title", "")
        text = str(title).strip()
        if not text:
            continue
        if len(text) > 140:
            text = text[:140].rstrip()
        if text in seen:
            continue
        seen.add(text)
        lines.append(text)
        if len(lines) >= max_items:
            break
    return lines


def _line_candidates(text: str) -> list[str]:
    out: list[str] = []
    for raw in text.splitlines():
        value = raw.strip()
        if value:
            out.append(value)
    return out


def _infer_heading_candidates(lines: list[str], *, max_items: int = 6) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in lines:
        compact = " ".join(value.split())
        if not compact:
            continue
        if compact in seen:
            continue
        words = compact.split()
        if not (2 <= len(words) <= 16):
            continue
        if compact.startswith(("*", "-", "•", "|", "_")):
            continue
        if ":" in compact:
            continue
        if not any(ch.isalpha() for ch in compact):
            continue
        if compact.isupper() or compact.istitle() or compact.lower().startswith("example"):
            out.append(compact)
            seen.add(compact)
        if len(out) >= max_items:
            break
    return out


def _infer_table_rows(lines: list[str], *, max_items: int = 40) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw in lines:
        value = " ".join(raw.replace("\t", " ").split())
        if not value:
            continue
        left = ""
        right = ""
        if ":" in value:
            left, right = value.split(":", 1)
            left = left.strip(" *-_")
            right = right.strip()
        else:
            parts = [item.strip() for item in re.split(r"\s{2,}", raw.strip()) if item.strip()]
            if len(parts) >= 2:
                left = parts[0]
                right = " | ".join(parts[1:])

        if left and 2 <= len(left) <= 96 and any(ch.isalpha() for ch in left):
            row = (left, right)
            if row not in seen:
                out.append(row)
                seen.add(row)
        if len(out) >= max_items:
            break

    if len(out) >= 3:
        return out

    for index, value in enumerate(lines, start=1):
        compact = " ".join(value.split())
        if len(compact) < 24 or compact.startswith(("*", "-", "•", "|", "_")):
            continue
        row = (f"Line {index}", compact)
        if row not in seen:
            out.append(row)
            seen.add(row)
        if len(out) >= min(max_items, 10):
            break
    return out


def enrich_text_with_structure(text: str, *, outline_headings: list[str] | None = None) -> str:
    body = text.strip()
    lines = _line_candidates(body)

    headings = list(outline_headings or [])
    if not headings:
        headings = _infer_heading_candidates(lines)
    heading_lines = [f"# {value}" for value in headings if value.strip()]

    table_rows = _infer_table_rows(lines)
    table_lines: list[str] = []
    if table_rows:
        table_lines.append("| Field | Value |")
        table_lines.append("| --- | --- |")
        for key, val in table_rows:
            safe_key = key.replace("|", "/").strip()
            safe_val = val.replace("|", "/").strip()
            table_lines.append(f"| {safe_key} | {safe_val} |")

    parts: list[str] = []
    if heading_lines:
        parts.append("\n".join(heading_lines))
    if body:
        parts.append(body)
    if table_lines:
        parts.append("\n".join(table_lines))
    return "\n\n".join(parts).strip()
