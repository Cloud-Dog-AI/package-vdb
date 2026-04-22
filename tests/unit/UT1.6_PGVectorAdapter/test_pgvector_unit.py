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

from cloud_dog_vdb.adapters.pgvector import PGVectorAdapter
from cloud_dog_vdb.config.models import ProviderConfig


def test_pgvector_capabilities_delete_by_filter_true():
    a = PGVectorAdapter(ProviderConfig(provider_id="pgvector", database_uri="postgresql://x"))
    assert a.capabilities().delete_by_filter is True
