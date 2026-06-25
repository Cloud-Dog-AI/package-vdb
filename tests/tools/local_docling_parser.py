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

"""Local docling parser command for platform-vdb parser-provider tests."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from docling_core.types.doc.page import TextCellUnit
from docling_parse.pdf_parser import DoclingPdfParser
from pypdf import PdfReader

from _enrichment import enrich_text_with_structure, outline_headings_from_reader


def _extract_outline_headings(path: Path) -> list[str]:
    reader = PdfReader(str(path))
    return outline_headings_from_reader(reader)


def _parse_pdf(path: Path) -> str:
    parser = DoclingPdfParser(loglevel="fatal")
    document = parser.load(path)
    lines: list[str] = []
    for _, page in document.iterate_pages():
        page_lines = page.export_to_textlines(
            TextCellUnit.LINE,
            add_location=False,
            add_fontname=False,
            add_text_direction=False,
        )
        lines.extend(str(item).strip() for item in page_lines if str(item).strip())
    body = "\n".join(lines).strip()
    headings = _extract_outline_headings(path)
    return enrich_text_with_structure(body, outline_headings=headings)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Parse PDF into plain text using docling-parse.")
    parser.add_argument("source", type=Path, help="Path to PDF file")
    args = parser.parse_args(argv)
    source: Path = args.source
    if not source.is_file():
        print(f"input file not found: {source}", file=sys.stderr)
        return 2
    text = _parse_pdf(source)
    if text:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
