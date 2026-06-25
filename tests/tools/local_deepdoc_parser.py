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

"""Local deepdoc parser command for platform-vdb parser-provider tests."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pypdf import PdfReader

from _enrichment import enrich_text_with_structure, outline_headings_from_reader


def _extract_with_pypdf(path: Path) -> str:
    reader = PdfReader(str(path))
    chunks: list[str] = []
    for page in reader.pages:
        chunks.append(page.extract_text() or "")
    body = "\n".join(item.strip() for item in chunks if item and item.strip()).strip()
    headings = outline_headings_from_reader(reader)
    return enrich_text_with_structure(body, outline_headings=headings)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Parse PDF into plain text for deepdoc provider tests.")
    parser.add_argument("source", type=Path, help="Path to PDF file")
    args = parser.parse_args(argv)
    source: Path = args.source
    if not source.is_file():
        print(f"input file not found: {source}", file=sys.stderr)
        return 2

    text = _extract_with_pypdf(source)
    if text:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
