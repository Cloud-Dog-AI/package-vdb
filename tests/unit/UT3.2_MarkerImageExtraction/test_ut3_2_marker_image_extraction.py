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

import base64

from cloud_dog_vdb.ingestion.parse.providers.marker_mcp import _coerce_marker_images


def test_ut3_2_marker_image_refs_decoded_and_preserved() -> None:
    encoded = base64.b64encode(b"fake-jpeg-bytes").decode("ascii")
    payload = {
        "success": True,
        "output": "# Report",
        "images": {
            "_page_0_Picture_0.jpeg": encoded,
            "_page_1_Picture_1.jpeg": encoded,
        },
    }

    refs = _coerce_marker_images(payload)
    assert len(refs) == 2
    assert refs[0]["ref"] == "_page_0_Picture_0.jpeg"
    assert refs[0]["encoding"] == "base64"
    assert refs[0]["mime_type"] == "image/jpeg"
    assert refs[0]["data_base64"] == encoded
    assert refs[0]["byte_size"] > 0
