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

"""Local transformers parser command for platform-vdb parser-provider tests."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pypdf import PdfReader
from transformers import AutoTokenizer

from _enrichment import enrich_text_with_structure, outline_headings_from_reader


def _extract_text(path: Path) -> str:
    reader = PdfReader(str(path))
    chunks: list[str] = []
    for page in reader.pages:
        chunks.append(page.extract_text() or "")
    body = "\n".join(item.strip() for item in chunks if item and item.strip()).strip()
    headings = outline_headings_from_reader(reader)
    return enrich_text_with_structure(body, outline_headings=headings)


def _tokenise_normalise(text: str) -> str:
    if not text.strip():
        return text
    model_id = str(os.environ.get("TRANSFORMERS_LOCAL_MODEL", "")).strip()
    if not model_id:
        return text
    try:
        tokenizer = AutoTokenizer.from_pretrained(model_id, local_files_only=True)
        token_ids = tokenizer.encode(text, truncation=True, max_length=4096)
        decoded = tokenizer.decode(token_ids, skip_special_tokens=True).strip()
        return decoded or text
    except Exception:
        return text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Parse PDF into plain text using pypdf + transformers tokenizer normalisation."
    )
    parser.add_argument("source", type=Path, help="Path to PDF file")
    args = parser.parse_args(argv)
    source: Path = args.source
    if not source.is_file():
        print(f"input file not found: {source}", file=sys.stderr)
        return 2
    text = _extract_text(source)
    if text:
        sys.stdout.write(_tokenise_normalise(text))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
