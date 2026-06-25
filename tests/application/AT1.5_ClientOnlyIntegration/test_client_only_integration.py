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

import asyncio
import json
import socketserver
import threading
import uuid
from http.server import BaseHTTPRequestHandler

import pytest

from cloud_dog_vdb import CollectionSpec, Record, SearchRequest, get_vdb_client
from cloud_dog_vdb.adapters.chroma import ChromaAdapter
from cloud_dog_vdb.config.models import ProviderConfig
from cloud_dog_vdb.remote.client import VDBClient


def _meta(tenant: str, source: str) -> dict:
    return {
        "tenant_id": tenant,
        "source_uri": source,
        "source_type": "web",
        "lifecycle_state": "active",
        "created_at": "2026-01-01T00:00:00Z",
    }


class _ThreadedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


class _ProxyHandler(BaseHTTPRequestHandler):
    chroma_cfg = None

    def log_message(self, format, *args):  # noqa: A003
        return

    def _send_json(self, status: int, payload: dict, headers: dict | None = None) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            self._send_json(200, {"ok": True})
            return
        self._send_json(404, {"error": "not_found"})

    def do_POST(self):  # noqa: N802
        if not self.path.startswith("/collections/") or not self.path.endswith("/search"):
            self._send_json(404, {"error": "not_found"})
            return
        parts = self.path.split("/")
        if len(parts) != 4:
            self._send_json(400, {"error": "invalid_path"})
            return
        collection = parts[2]
        size = int(self.headers.get("Content-Length", "0") or "0")
        payload = json.loads(self.rfile.read(size).decode("utf-8") or "{}")
        req = SearchRequest(query_text=str(payload.get("query_text", "")), top_k=int(payload.get("top_k", 10) or 10))

        async def _search() -> list[dict]:
            adapter = ChromaAdapter(
                ProviderConfig(
                    provider_id="chroma",
                    base_url=self.chroma_cfg.get("base_url", ""),
                    api_key=self.chroma_cfg.get("auth_token", ""),
                ),
                local_mode=False,
            )
            try:
                adapter._dims[collection] = 4
                hits = await adapter.search(collection, req.query_text, req.top_k, dict(payload.get("filters", {})), {})
                return [
                    {
                        "id": item.get("id", ""),
                        "score": float(item.get("score", 0.0)),
                        "content": item.get("content", ""),
                        "metadata": item.get("metadata", {}),
                    }
                    for item in hits
                ]
            finally:
                await adapter._client.aclose()

        results = asyncio.run(_search())
        self._send_json(200, {"results": results}, headers={"X-VDB-Backend": "portable"})


@pytest.mark.asyncio
async def test_client_only_remote_integration_search(chroma_ready):
    cfg = chroma_ready
    collection = f"cloud_dog_ai_remote_proxy_{uuid.uuid4().hex[:10]}"
    vdb = get_vdb_client(
        {
            "vector_stores": {
                "default_backend": "chroma",
                "chroma": {
                    "enabled": True,
                    "local_mode": False,
                    "base_url": cfg.get("base_url", ""),
                    "auth_token": cfg.get("auth_token", ""),
                },
            }
        }
    )
    await vdb.create_collection(CollectionSpec(name=collection, embedding_dim=4))
    await vdb.upsert_records(
        collection,
        [Record("r1", "hello remote integration", _meta("t1", "https://example.net/remote"))],
    )

    _ProxyHandler.chroma_cfg = cfg
    server = _ThreadedHTTPServer(("127.0.0.1", 0), _ProxyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]

    client = VDBClient(f"http://127.0.0.1:{port}", backend_hint="portable")
    try:
        assert await client.health_check() is True
        result = await client.search(collection, SearchRequest(query_text="hello", top_k=1))
        assert isinstance(result.results, list)
        assert result.results and result.results[0].id == "r1"
    finally:
        await client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)
        await vdb.delete_collection(collection)
