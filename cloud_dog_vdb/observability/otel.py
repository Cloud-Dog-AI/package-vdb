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

from contextlib import contextmanager


def trace_enabled(*, enabled: bool = False) -> bool:
    """Feature flag hook.

    The caller passes this from resolved platform config; no local env reads.
    """
    return bool(enabled)


@contextmanager
def trace_span(name: str):
    """No-op span context with an OTel-compatible API shape."""
    _ = name
    yield
