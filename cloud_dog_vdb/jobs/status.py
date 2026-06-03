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


def progress(done: int, total: int) -> float:
    """Handle progress."""
    if total <= 0:
        return 0.0
    return max(0.0, min(1.0, done / total))


def status_from_progress(done: int, total: int) -> str:
    """Handle status from progress."""
    ratio = progress(done, total)
    if ratio <= 0.0:
        return "queued"
    if ratio >= 1.0:
        return "completed"
    return "running"
