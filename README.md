# platform-vdb

**Package:** `cloud_dog_vdb`  
**Standard:** PS-60 (Vector DB Interfaces)  
**Status:** Active (0.5.0)

## Purpose

Drop-in Python library implementing the PS-60 vector DB interfaces standard. Provides pluggable backend adapters, canonical metadata enforcement, deterministic identity, ingestion pipelines, job control, capability-aware query planning, and lifecycle management.

## Key Features

- **Backend adapters**: Chroma (local+remote), Qdrant, Weaviate (v4+v3), OpenSearch, PGVector — all behind `VDBAdapter` interface
- **Capability discovery**: Descriptor framework, capability-aware query/operation planning
- **Canonical metadata**: Schema validation, deterministic ID/hash computation (SHA-256)
- **Options management**: Common indexing/search options + backend-specific (`x_backend.*`)
- **CRUD**: Collection management + record upsert/search/delete/update with portable filters
- **Lifecycle**: Soft-delete, supersession, retention policies, purge safety checks
- **Ingestion pipeline**: Acquire → convert → chunk → embed → upsert (pluggable stages)
- **Converters**: pandas, deepdoc, mineru, generic external (pluggable)
- **Chunkers**: Fixed token, recursive splitter, semantic (pluggable)
- **Parser ecosystem uplift (0.4.0)**: parser chains for internal/external providers (MinerU, marker-mcp, DeepDoc, Docling, Transformers), optional OCR modes (`disabled|auto|force`), table handling policies, and Document IR provenance.
- **MinerU resilience (0.4.0)**: adaptive retry policy for transient transport/5xx failures, low-VRAM retry payloads, and optional page-sliced fallback for PDF workloads under GPU pressure.
- **Backend uplift (0.4.0)**: Infinity VDB adapter support (config from `dev.vdbs.infinity`) with capability-aware planning and conformance coverage.
- **Job control**: Job model, queue interface, checkpoints, worker hooks
- **Search**: Vector similarity, hybrid (BM25+vector), metadata filtering, streaming results, reranking hooks
- **Access controls**: Tenant/namespace isolation, role-based collection access
- **Config delegation**: All credentials pre-resolved by `cloud_dog_config` (PS-80) — no Vault/env reads in this package
- **Compatibility normaliser**: Backend response normalisation to portable `SearchResponse`
- **Client-only mode**: Lightweight remote VDB proxy for applications without local adapters
- **Schema versioning**: Collection schema version tracking with mismatch warnings and migration planning
- **Observability**: Audit events, metrics hooks, OpenTelemetry integration
- **Optional integrations**: LlamaIndex, LangChain adapter layers

## Dependencies

- **Required:** `httpx`, `pypdf`, `sqlalchemy`
- **Optional per backend:** `chromadb`, `qdrant-client`, `weaviate-client`, `opensearch-py`, `asyncpg`+`pgvector`
- **Optional pipeline:** `pandas`, `sqlalchemy`

## Documents

- [REQUIREMENTS.md](REQUIREMENTS.md) — 36 functional requirements
- [ARCHITECTURE.md](ARCHITECTURE.md) — Module layout, component design, integration pattern
- [TESTS.md](TESTS.md) — Test plan, directory structure, coverage map (UT/ST/IT/AT + UT2/ST2/IT2/PT/QT2/CT1)
- [RELEASE_UPLIFT_PROPOSAL.md](RELEASE_UPLIFT_PROPOSAL.md) — 0.4.0 parser/OCR/table/Infinity uplift scope and migration notes
- [PROGRAMME-0.4.0-DEVELOPMENT-BUILD-TEST.md](PROGRAMME-0.4.0-DEVELOPMENT-BUILD-TEST.md) — Development, build, and test execution plan for 0.4.0
- [AGENTS.md](AGENTS.md) — Agent runbook for compliant implementation, validation, and release evidence updates

## 0.4.0 Closeout Evidence (2026-03-01)

- W13A strict closeout gate sequence completed with real dependencies and live endpoints.
- Parser provider strict enforcement (`IT2.11`, `AT2.2`, `REQUIRE_ALL_PDF_PARSERS=true`) passes with explicit mismatch diagnostics.
- Large-corpus parser and parser-performance gates pass under timeout guards (`PT1`, `PT2`).
- Build/install/smoke checks pass for `cloud_dog_vdb==0.4.0`.
- Detailed command evidence and first-failure traces are recorded in:
  - [TESTS.md](TESTS.md) run history
  - `working/W13A-PLATFORM-VDB-0.4.0-RELEASE-CLOSEOUT-REPORT-2026-03-01.md`

## 0.4.1 Packaging + Publish Evidence (2026-03-01)

- Package version bumped to `0.4.1` (`pyproject.toml`, `cloud_dog_vdb/__init__.py`).
- Build artifacts created:
  - `dist/cloud_dog_vdb-0.4.1-py3-none-any.whl`
  - `dist/cloud_dog_vdb-0.4.1.tar.gz`
- Local install + smoke check passes:
  - `.venv/bin/pip install --force-reinstall dist/cloud_dog_vdb-0.4.1-py3-none-any.whl`
  - `.venv/bin/python -c "import cloud_dog_vdb; print(cloud_dog_vdb.__version__)"`
  - Output: `0.4.1`
- Published to private PyPI:
  - `<your-package-index>`

## Test Corpus

The package includes real-world parser/OCR test documents in `test-data/` (PDF, table-heavy, scanned, mixed, and handwritten forms) used for parser quality and performance validation.

## Marker MCP Configuration (0.5.0)

Marker parser transport uses MCP JSON-RPC over streamable HTTP (`/mcp`) with session initialisation and `tools/call`.

Required/optional env keys:

- `MARKER_MCP_ENABLED=true`
- `MARKER_MCP_BASE_URL=https://marker.example.com`
- `MARKER_MCP_AUTH_TOKEN=` (optional when service keying is disabled)
- `MARKER_MCP_TIMEOUT_SECONDS` (default profile-dependent)
- `MARKER_MCP_REQUEST_RETRIES` (default profile-dependent)
- `MARKER_MCP_ASYNC_THRESHOLD_SECONDS` (controls async fallback threshold)
- `MARKER_MCP_ASYNC_POLL_INTERVAL_SECONDS`
- `MARKER_MCP_ASYNC_MAX_WAIT_SECONDS`

Request headers for MCP transport:

- `Accept: application/json, text/event-stream`
- `X-API-Key: <token>` (if auth token configured)

## Quick Start

```python
from cloud_dog_vdb import CollectionSpec, get_vdb_client

vdb = get_vdb_client(config)
await vdb.create_collection(CollectionSpec(name="cloud_dog_ai_example"), provider_id="chroma")
details = await vdb.get_collection("cloud_dog_ai_example", provider_id="chroma")
await vdb.delete_collection("cloud_dog_ai_example", provider_id="chroma")
```

## Installation

```bash
pip install cloud-dog-vdb
```

## API Overview

- provider factories build vector-store clients from shared config
- ingestion helpers process content into chunked records
- search helpers standardise retrieval queries and results

## Examples

- Create a vector-store client from shared provider config.
- Run ingestion and retrieval flows through the common adapter surface.

---

## Licence

Apache-2.0 — Copyright (c) 2026 Cloud-Dog, Viewdeck Engineering Limited
