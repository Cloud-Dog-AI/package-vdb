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

from cloud_dog_vdb.ingestion.parse.base import ParserProvider
from cloud_dog_vdb.ingestion.parse.async_runner import AsyncParseRunner
from cloud_dog_vdb.ingestion.parse.capabilities import ParserCapabilities
from cloud_dog_vdb.ingestion.parse.ir import DocumentIR, TableBlock, TextBlock
from cloud_dog_vdb.ingestion.parse.planner import build_fallback_chain, pick_first_available
from cloud_dog_vdb.ingestion.parse.providers.deepdoc import DeepDocParserProvider
from cloud_dog_vdb.ingestion.parse.providers.docling import DoclingParserProvider
from cloud_dog_vdb.ingestion.parse.providers.internal import InternalParserProvider
from cloud_dog_vdb.ingestion.parse.providers.marker_mcp import MarkerMcpParserProvider
from cloud_dog_vdb.ingestion.parse.providers.mineru import MineruParserProvider
from cloud_dog_vdb.ingestion.parse.providers.transformers import TransformersParserProvider
from cloud_dog_vdb.ingestion.parse.quality import quality_gate_passed
from cloud_dog_vdb.ingestion.parse.registry import ParserRegistry

__all__ = [
    "ParserProvider",
    "AsyncParseRunner",
    "ParserCapabilities",
    "DocumentIR",
    "TextBlock",
    "TableBlock",
    "ParserRegistry",
    "pick_first_available",
    "build_fallback_chain",
    "quality_gate_passed",
    "InternalParserProvider",
    "MineruParserProvider",
    "MarkerMcpParserProvider",
    "DeepDocParserProvider",
    "DoclingParserProvider",
    "TransformersParserProvider",
]
