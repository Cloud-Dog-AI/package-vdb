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

"""Backend matrix (requirements section 19.2): every representative workbook is
extracted and round-tripped (upsert -> count -> search -> delete) through each
vector backend, exercised locally via the built-in in-memory ``local_mode`` so
no external infrastructure is required and record generation stays backend-neutral.
"""

from __future__ import annotations

import pytest
from cloud_dog_vdb import CollectionSpec, SearchRequest, get_vdb_client
from cloud_dog_vdb.spreadsheet import extract_workbook, searchable_records_to_records, testing

# Section 19.2 backend matrix.
BACKENDS = ["chroma", "qdrant", "weaviate", "opensearch", "pgvector"]

# Representative workbooks across the section 19.1 coverage dimensions.
WORKBOOKS = [
    ("simple", testing.build_simple_xlsx, "simple.xlsx"),
    ("multisheet", testing.build_multisheet_formal_tables_xlsx, "multi.xlsx"),
    ("inferred", testing.build_inferred_only_xlsx, "inferred.xlsx"),
    ("formula_named", testing.build_formula_named_range_xlsx, "calc.xlsx"),
    ("hidden", testing.build_hidden_sheet_xlsx, "hidden.xlsx"),
    ("multilingual", testing.build_multilingual_xlsx, "ml.xlsx"),
    ("report_grid", testing.build_report_grid_xlsx, "grid.xlsx"),
    ("ods", testing.build_simple_ods, "budget.ods"),
]


def _client(backend: str):
    return get_vdb_client(
        {"vector_stores": {"default_backend": backend, backend: {"enabled": True, "local_mode": True}}}
    )


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("workbook", WORKBOOKS, ids=[w[0] for w in WORKBOOKS])
async def test_workbook_roundtrips_through_backend(backend, workbook):
    name, builder, file_name = workbook
    extraction = extract_workbook(builder(), file_name=file_name, source_uri=f"file:///{file_name}")
    records = searchable_records_to_records(extraction.searchable_records)
    assert records, f"{name}: expected searchable records"

    client = _client(backend)
    collection = f"ssmatrix_{backend}_{name}"
    await client.create_collection(CollectionSpec(name=collection, embedding_dim=16), provider_id=backend)

    upserted = await client.upsert_records(collection, records, provider_id=backend)
    assert len(upserted) == len(records)

    count = await client.count_documents(collection, provider_id=backend)
    assert count == len(records)

    response = await client.search(
        collection, SearchRequest(query_text=name, top_k=5), provider_id=backend
    )
    assert response.results, f"{name}@{backend}: search returned no results"
    # every returned hit carries the spreadsheet source_type
    assert all(r.payload.get("metadata", r.payload).get("source_type") == "excel" for r in response.results)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_delete_key_prunes_record_across_backends(backend):
    extraction = extract_workbook(testing.build_simple_xlsx(), file_name="s.xlsx", source_uri="file:///s.xlsx")
    records = searchable_records_to_records(extraction.searchable_records)
    client = _client(backend)
    collection = f"ssdelete_{backend}"
    await client.create_collection(CollectionSpec(name=collection, embedding_dim=16), provider_id=backend)
    await client.upsert_records(collection, records, provider_id=backend)

    victim = records[0].record_id
    assert await client.delete_record(collection, victim, provider_id=backend)
    assert await client.count_documents(collection, provider_id=backend) == len(records) - 1
