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

from cloud_dog_vdb.lifecycle.manager import mark_deleted, mark_superseded


def test_lifecycle_real_backend_pattern():
    assert mark_deleted({"id": "1"})["lifecycle_state"] == "deleted"
    assert mark_superseded({"id": "1"})["lifecycle_state"] == "superseded"
