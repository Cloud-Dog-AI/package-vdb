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

import os


def test_pt1_6_env_endpoint_values_are_readable() -> None:
    required_keys = {
        "MINERU_ENABLED",
        "MINERU_BASE_URL",
        "MARKER_MCP_ENABLED",
        "MARKER_MCP_BASE_URL",
        "DEEPDOC_ENABLED",
        "DEEPDOC_COMMAND",
        "DOCLING_ENABLED",
        "DOCLING_COMMAND",
        "TRANSFORMERS_ENABLED",
    }
    for key in required_keys:
        assert key in os.environ, f"Missing parser env key: {key}"
