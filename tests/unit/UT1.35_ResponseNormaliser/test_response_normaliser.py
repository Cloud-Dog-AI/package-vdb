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

from cloud_dog_vdb.compat.response_normaliser import ResponseNormaliser


def test_response_normaliser_maps_all_backends():
    n = ResponseNormaliser()

    chroma = n.normalise_search(
        "chroma",
        {
            "ids": [["c1"]],
            "documents": [["doc"]],
            "metadatas": [[{"tenant_id": "t1"}]],
            "distances": [[0.2]],
        },
    )
    assert chroma.results[0].id == "c1"
    assert chroma.results[0].payload["metadata"]["tenant_id"] == "t1"

    qdrant = n.normalise_search(
        "qdrant",
        {"result": [{"id": "q1", "score": 0.9, "payload": {"text": "doc", "metadata": {"a": 1}}}]},
    )
    assert qdrant.results[0].id == "q1"

    weaviate = n.normalise_search(
        "weaviate",
        [{"external_id": "w1", "text": "doc", "_additional": {"distance": 0.1}, "metadata": {"b": 2}}],
    )
    assert weaviate.results[0].id == "w1"

    opensearch = n.normalise_search(
        "opensearch",
        {"hits": {"hits": [{"_id": "o1", "_score": 1.5, "_source": {"text": "doc", "metadata": {"c": 3}}}]}},
    )
    assert opensearch.results[0].id == "o1"

    pg = n.normalise_search("pgvector", {"rows": [{"id": "p1", "score": 0.88, "content": "doc", "metadata": {}}]})
    assert pg.results[0].id == "p1"


def test_response_normaliser_unknown_backend_raises():
    n = ResponseNormaliser()
    try:
        n.normalise_search("unknown", {})
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
