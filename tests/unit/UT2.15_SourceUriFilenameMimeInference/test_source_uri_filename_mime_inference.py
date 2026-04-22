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

from cloud_dog_vdb import CollectionSpec, get_vdb_client
from cloud_dog_vdb.domain.errors import InvalidRequestError
from cloud_dog_vdb.ingestion.parse.base import ParserProvider
from cloud_dog_vdb.ingestion.parse.capabilities import ParserCapabilities
from cloud_dog_vdb.ingestion.parse.ir import DocumentIR, TextBlock
from cloud_dog_vdb.ingestion.parse.providers.internal import InternalParserProvider
from cloud_dog_vdb.ingestion.parse.registry import ParserRegistry
from cloud_dog_vdb.ingestion.pipeline import ParserIngestionOptions, ingest_document


class _PdfAwareParser(ParserProvider):
    provider_id = "pdfaware"
    provider_version = "test-1"

    def __init__(self) -> None:
        self.last_filename = ""
        self.last_mime_type = ""

    @property
    def capabilities(self) -> ParserCapabilities:
        return ParserCapabilities(
            supports_pdf=True,
            supports_docx=False,
            supports_html=False,
            supports_layout=False,
            supports_tables=False,
            supports_images=False,
            supports_ocr_passthrough=False,
            supports_streaming=False,
            max_document_bytes=1024 * 1024,
        )

    async def health_check(self) -> bool:
        return True

    async def parse_bytes(
        self,
        document: bytes,
        *,
        filename: str,
        source_uri: str,
        mime_type: str | None = None,
        options: dict | None = None,
    ) -> DocumentIR:
        del document, source_uri, options
        self.last_filename = filename
        self.last_mime_type = str(mime_type or "")
        if not filename.lower().endswith(".pdf"):
            raise InvalidRequestError("pdfaware requires .pdf filename")
        if self.last_mime_type != "application/pdf":
            raise InvalidRequestError("pdfaware requires application/pdf mime_type")
        return DocumentIR(
            source_uri="file://sample.pdf",
            provider_id=self.provider_id,
            provider_version=self.provider_version,
            text_blocks=[TextBlock(text="pdfaware parser output")],
        )


class _OptionAwareParser(ParserProvider):
    provider_id = "mineru"
    provider_version = "test-1"

    def __init__(self) -> None:
        self.last_options: dict = {}

    @property
    def capabilities(self) -> ParserCapabilities:
        return ParserCapabilities(
            supports_pdf=True,
            supports_docx=True,
            supports_html=True,
            supports_layout=False,
            supports_tables=False,
            supports_images=False,
            supports_ocr_passthrough=True,
            supports_streaming=False,
            max_document_bytes=1024 * 1024,
        )

    async def health_check(self) -> bool:
        return True

    async def parse_bytes(
        self,
        document: bytes,
        *,
        filename: str,
        source_uri: str,
        mime_type: str | None = None,
        options: dict | None = None,
    ) -> DocumentIR:
        del document, filename, source_uri, mime_type
        self.last_options = dict(options or {})
        return DocumentIR(
            source_uri="file://sample.pdf",
            provider_id=self.provider_id,
            provider_version=self.provider_version,
            text_blocks=[TextBlock(text="option aware parser output")],
        )


@pytest.mark.asyncio
async def test_ingest_document_inferrs_filename_and_mime_from_source_uri_for_bytes() -> None:
    provider = _PdfAwareParser()
    registry = ParserRegistry()
    registry.register(provider)
    registry.register(InternalParserProvider())
    client = get_vdb_client(
        {"vector_stores": {"default_backend": "chroma", "chroma": {"enabled": True, "local_mode": True}}}
    )
    collection = "ut2_15_source_uri_filename_mime_inference"
    await client.create_collection(CollectionSpec(name=collection, embedding_dim=4))

    ids = await ingest_document(
        client,
        collection,
        b"%PDF-1.4\nfake\n",
        source_uri="file://sample.pdf",
        parser_registry=registry,
        options=ParserIngestionOptions(parser_chain=["pdfaware", "internal"]),
    )

    records = await client.list_records(collection, {})
    assert ids
    assert records
    assert provider.last_filename == "sample.pdf"
    assert provider.last_mime_type == "application/pdf"
    assert str(records[0].metadata.get("parser_provider", "")) == "pdfaware"


@pytest.mark.asyncio
async def test_ingest_document_maps_additive_parser_flags_to_provider_options() -> None:
    provider = _OptionAwareParser()
    registry = ParserRegistry()
    registry.register(provider)
    registry.register(InternalParserProvider())
    client = get_vdb_client(
        {"vector_stores": {"default_backend": "chroma", "chroma": {"enabled": True, "local_mode": True}}}
    )
    collection = "ut2_15_parser_flag_mapping"
    await client.create_collection(CollectionSpec(name=collection, embedding_dim=4))

    ids = await ingest_document(
        client,
        collection,
        b"%PDF-1.4\nfake\n",
        source_uri="file://sample.pdf",
        parser_registry=registry,
        options=ParserIngestionOptions(
            parser_chain=["internal"],
            parser_provider="mineru",
            quality="fast",
            ocr_enabled=False,
            page_fallback_target_chars=600,
            page_fallback_max_pages=3,
        ),
    )

    records = await client.list_records(collection, {})
    assert ids
    assert records
    assert provider.last_options["parse_backend"] == "pipeline"
    assert provider.last_options["parse_method"] == "txt"
    assert provider.last_options["formula_enable"] is False
    assert provider.last_options["table_enable"] is False
    assert provider.last_options["page_fallback_target_chars"] == 600
    assert provider.last_options["page_fallback_max_pages"] == 3
    assert str(records[0].metadata.get("parser_provider", "")) == "mineru"
