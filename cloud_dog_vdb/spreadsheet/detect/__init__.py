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

"""Region detection, classification and column profiling (sections 5.5, 5.6, 12)."""

from __future__ import annotations

from cloud_dog_vdb.spreadsheet.detect.classify import classify_region
from cloud_dog_vdb.spreadsheet.detect.profile import ColumnProfile, infer_data_type, profile_column
from cloud_dog_vdb.spreadsheet.detect.regions import DetectedRegion, detect_regions

__all__ = [
    "ColumnProfile",
    "DetectedRegion",
    "classify_region",
    "detect_regions",
    "infer_data_type",
    "profile_column",
]
