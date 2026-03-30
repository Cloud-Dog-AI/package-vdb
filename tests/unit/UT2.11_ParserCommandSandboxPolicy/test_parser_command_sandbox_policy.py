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


def _is_allowed_command(command: str, allowlist: set[str]) -> bool:
    head = command.strip().split(" ", 1)[0]
    return head in allowlist


def test_ut2_11_command_allowlist_policy() -> None:
    allowlist = {"mineru", "docling", "pandoc"}
    assert _is_allowed_command("mineru parse input.pdf", allowlist)
    assert not _is_allowed_command("bash -lc 'echo hi'", allowlist)
