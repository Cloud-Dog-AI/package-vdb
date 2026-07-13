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

"""Multilingual / i18n support for spreadsheet indexing (requirements section 5.19).

Language is detected from the dominant Unicode script — a dependency-free
heuristic suitable for short schema text (sheet/table/column names). Original
text is always preserved; detection only annotates records for language-aware
routing and search boosting. Translated aliases are produced only when the
caller supplies a translation function (no provider is bundled).
"""

from __future__ import annotations

from collections.abc import Callable
from collections import Counter

# Unicode codepoint ranges → language code (priority order matters: kana before han).
_SCRIPT_RANGES: tuple[tuple[int, int, str], ...] = (
    (0x3040, 0x30FF, "ja"),  # Hiragana + Katakana
    (0xAC00, 0xD7AF, "ko"),  # Hangul syllables
    (0x1100, 0x11FF, "ko"),  # Hangul jamo
    (0x4E00, 0x9FFF, "zh"),  # CJK unified ideographs
    (0x3400, 0x4DBF, "zh"),  # CJK extension A
    (0x0590, 0x05FF, "he"),  # Hebrew
    (0x0600, 0x06FF, "ar"),  # Arabic
    (0x0750, 0x077F, "ar"),  # Arabic supplement
    (0x0400, 0x04FF, "ru"),  # Cyrillic
    (0x0370, 0x03FF, "el"),  # Greek
)


def _char_language(char: str) -> str | None:
    code = ord(char)
    for start, end, lang in _SCRIPT_RANGES:
        if start <= code <= end:
            return lang
    if char.isalpha():
        return "en"  # Latin and other alphabetic scripts default to English/Latin
    return None


def detect_language(text: str) -> str:
    """Detect the dominant language of ``text`` by script (section 5.19).

    Returns an ISO-639-1-style code (``en``/``zh``/``ja``/``ko``/``ar``/``he``/
    ``ru``/``el``), or ``und`` when no alphabetic characters are present.
    """
    counts: Counter[str] = Counter()
    for char in text:
        lang = _char_language(char)
        if lang is not None:
            counts[lang] += 1
    if not counts:
        return "und"
    # Kana and Hangul are exclusive to Japanese / Korean, so any presence is
    # definitive even when Han (shared with Chinese) characters dominate by count.
    if counts.get("ja"):
        return "ja"
    if counts.get("ko"):
        return "ko"
    # Otherwise the dominant script wins; non-Latin breaks ties over incidental Latin.
    best = max(counts.items(), key=lambda kv: (kv[1], kv[0] != "en"))
    return best[0]


def dominant_language(texts: list[str]) -> str:
    """Return the most common detected language across ``texts`` (section 5.19)."""
    counts: Counter[str] = Counter()
    for text in texts:
        lang = detect_language(text)
        if lang != "und":
            counts[lang] += 1
    if not counts:
        return "und"
    return counts.most_common(1)[0][0]


def translated_aliases(names: list[str], translator: Callable[[str], str]) -> dict[str, str]:
    """Return ``{original: translated}`` aliases using a caller-supplied translator.

    Used only when ``generate_translated_aliases`` is enabled and a translator is
    available; failures per name are skipped so indexing never breaks (section 5.19).
    """
    aliases: dict[str, str] = {}
    for name in names:
        if not name:
            continue
        try:
            translated = translator(name)
        except Exception:  # noqa: BLE001 - translation is best-effort
            continue
        if translated and translated != name:
            aliases[name] = translated
    return aliases
