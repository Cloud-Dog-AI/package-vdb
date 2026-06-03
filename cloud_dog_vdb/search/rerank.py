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

from collections.abc import Callable


def rerank(results: list[dict], key: Callable[[dict], float] | None = None, limit: int | None = None) -> list[dict]:
    """Rerank search hits using score-first ordering.

    When no key is supplied the function uses `score` descending and keeps input
    order as a tie-breaker.
    """
    rank_key = key or (lambda item: float(item.get("score", 0.0)))
    ranked = sorted(enumerate(results), key=lambda pair: (-rank_key(pair[1]), pair[0]))
    output = [item for _, item in ranked]
    return output[:limit] if isinstance(limit, int) and limit > 0 else output
