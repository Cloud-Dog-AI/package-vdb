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

import pytest
from cloud_dog_vdb.domain.models import Job
from cloud_dog_vdb.jobs.queue import JobQueue


@pytest.mark.asyncio
async def test_job_queue_submit_get_list():
    q = JobQueue()
    jid = await q.submit(Job(job_id="j1", job_type="ingest", status="queued"))
    assert jid == "j1" and (await q.get("j1")) is not None and len(await q.list()) == 1
