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

from cloud_dog_vdb.domain.models import Job


class JobQueue:
    """Represent job queue."""

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}

    async def submit(self, job: Job) -> str:
        """Handle submit."""
        self._jobs[job.job_id] = job
        return job.job_id

    async def get(self, job_id: str) -> Job | None:
        """Handle get."""
        return self._jobs.get(job_id)

    async def list(self) -> list[Job]:
        """Handle list."""
        return list(self._jobs.values())

    async def cancel(self, job_id: str) -> bool:
        """Handle cancel."""
        if job_id not in self._jobs:
            return False
        j = self._jobs[job_id]
        self._jobs[job_id] = Job(**{**j.__dict__, "status": "cancelled"})
        return True
