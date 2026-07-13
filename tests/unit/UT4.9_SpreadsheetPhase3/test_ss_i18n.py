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

from cloud_dog_vdb.spreadsheet import extract_workbook, testing
from cloud_dog_vdb.spreadsheet.config import SpreadsheetConfig
from cloud_dog_vdb.spreadsheet.i18n import detect_language, dominant_language, translated_aliases


def test_detect_language_by_script():
    assert detect_language("Revenue by Region") == "en"
    assert detect_language("商品名称") == "zh"
    assert detect_language("こんにちは") == "ja"
    assert detect_language("안녕하세요") == "ko"
    assert detect_language("مرحبا") == "ar"
    assert detect_language("שלום") == "he"
    assert detect_language("Привет") == "ru"
    assert detect_language("123 456") == "und"


def test_dominant_language():
    assert dominant_language(["商品", "名称", "hello"]) == "zh"


def test_translated_aliases_with_translator():
    aliases = translated_aliases(["Region", "Revenue"], translator=lambda s: f"{s}_xx")
    assert aliases == {"Region": "Region_xx", "Revenue": "Revenue_xx"}


def test_translator_failure_is_skipped():
    def boom(_):
        raise RuntimeError("no provider")

    assert translated_aliases(["A"], translator=boom) == {}


def test_workbook_language_recorded_on_records():
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx")
    assert ex.workbook.language == "en"
    assert all(r.language for r in ex.searchable_records)


def test_language_detection_can_be_disabled():
    ex = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", config=SpreadsheetConfig(detect_language=False))
    assert ex.workbook.language == ""
