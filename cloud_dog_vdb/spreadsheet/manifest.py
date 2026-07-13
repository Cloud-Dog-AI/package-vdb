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

"""Object manifest construction and refresh diffing (requirements section 5.18).

The manifest is a flat list of ``(object_key, object_hash)`` pairs describing one
workbook version. Comparing the previous and current manifests yields per-object
refresh actions (``unchanged``/``upsert``/``delete``) so re-indexing only touches
changed objects and deletes stale ones.
"""

from __future__ import annotations

from dataclasses import dataclass

from cloud_dog_vdb.spreadsheet.model import ObjectManifestEntry

#: Refresh actions (section 5.18 / 14.2 refresh_manifests.refresh_action).
REFRESH_ACTIONS = ("unchanged", "upsert", "delete", "rebuild")


@dataclass
class RefreshDecision:
    """A per-object refresh decision across two workbook versions (section 5.18)."""

    object_key: str
    refresh_action: str
    previous_object_hash: str = ""
    current_object_hash: str = ""
    reason: str = ""


def diff_manifests(
    previous: list[ObjectManifestEntry],
    current: list[ObjectManifestEntry],
) -> list[RefreshDecision]:
    """Diff two object manifests into per-object refresh decisions (section 5.18).

    Objects present only in ``current`` are ``upsert`` (new); objects whose hash
    changed are ``upsert`` (changed); objects present only in ``previous`` are
    ``delete`` (stale); identical objects are ``unchanged``.
    """
    prev_by_key = {entry.object_key: entry for entry in previous}
    curr_by_key = {entry.object_key: entry for entry in current}
    decisions: list[RefreshDecision] = []

    for key, entry in curr_by_key.items():
        prior = prev_by_key.get(key)
        if prior is None:
            decisions.append(RefreshDecision(key, "upsert", "", entry.object_hash, "new object"))
        elif prior.object_hash != entry.object_hash:
            decisions.append(RefreshDecision(key, "upsert", prior.object_hash, entry.object_hash, "content changed"))
        else:
            decisions.append(RefreshDecision(key, "unchanged", prior.object_hash, entry.object_hash, "no change"))

    for key, prior in prev_by_key.items():
        if key not in curr_by_key:
            decisions.append(RefreshDecision(key, "delete", prior.object_hash, "", "object removed"))
    return decisions
