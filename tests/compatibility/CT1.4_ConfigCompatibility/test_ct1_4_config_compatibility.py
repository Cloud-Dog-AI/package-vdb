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

from cloud_dog_vdb.runtime.factory import provider_config_from_dict


def test_ct1_4_legacy_config_fields_are_accepted() -> None:
    cfg = provider_config_from_dict("chroma", {"url": "http://localhost", "auth_token": "x"})
    assert cfg.base_url == "http://localhost"
    assert cfg.api_key == "x"
