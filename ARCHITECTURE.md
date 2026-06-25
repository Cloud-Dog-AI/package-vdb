# platform-vdb — Architecture

**Package:** `cloud_dog_vdb`  
**Version:** 0.5.0  
**Standard:** PS-60 (Vector DB Interfaces)  
**Status:** Active

---

## OV1 — Overview

`cloud_dog_vdb` is a drop-in Python library that implements the PS-60 vector DB interfaces standard. It provides pluggable backend adapters, canonical metadata enforcement, deterministic identity computation, ingestion pipelines, job control, capability-aware query planning, and lifecycle management — all behind stable, framework-agnostic interfaces.

### Design Goals

- **Ports-and-adapters**: common domain API; backend adapters translate to vendor APIs.
- **Capability-driven planning**: query/index behaviour depends on backend capabilities.
- **Pipeline as a job**: ingestion steps are observable, resumable, and queueable.
- **Metadata is first-class**: deterministic IDs/hashes prevent duplicates and support lifecycle.
- **Compatible with existing expert-agent**: clean migration path from current 5-provider implementation.

---

## SA1 — Module Layout

```
cloud_dog_vdb/
  __init__.py                          # Public API: VDBClient, get_vdb_client
  config/
    models.py                          # Pydantic settings for profiles, backends, pipelines
  domain/
    models.py                          # CollectionSpec, Record, SearchRequest/Response, Job
    errors.py                          # Portable error taxonomy
    enums.py                           # LifecycleState, SourceType, DistanceMetric, IndexType
  capabilities/
    models.py                          # CapabilityDescriptor
    planner.py                         # Capability-aware query/operation planner
  adapters/
    base.py                            # VDBAdapter interface (ABC)
    chroma.py                          # Chroma adapter (local + remote)
    qdrant.py                          # Qdrant adapter (HNSW, quantization)
    weaviate.py                        # Weaviate adapter (v4 + v3 fallback)
    opensearch.py                      # OpenSearch adapter (kNN, hybrid)
    pgvector.py                        # PGVector adapter (IVFFlat + HNSW)
    infinity.py                        # Infinity adapter
    factory.py                         # Adapter factory from ProviderConfig
    registry.py                        # Adapter registry (configure active backends)
    vector_utils.py                    # Deterministic vectors for tests/local mode
  options/
    common.py                          # CommonIndexingOptions, CommonSearchOptions
    chroma.py                          # ChromaOptions
    qdrant.py                          # QdrantOptions
    weaviate.py                        # WeaviateOptions
    opensearch.py                      # OpenSearchOptions
    pgvector.py                        # PGVectorOptions
    manager.py                         # VectorStoreOptionsManager (merge, resolve)
  metadata/
    schema.py                          # Canonical metadata validation
    identity.py                        # Deterministic ID/hash computation
    normalise.py                       # Source URI normalisation
  access/
    policy.py                          # Portable access policy model
    enforcement.py                     # Enforcement hooks (service-layer pre-checks)
  lifecycle/
    manager.py                         # Soft-delete, supersession, lifecycle transitions
    retention.py                       # TTL/retention policies, purge safety
  isolation/
    manager.py                         # Tenant/namespace isolation, cross-talk prevention
  ingestion/
    pipeline.py                        # Legacy + parser-first ingestion runtime
    acquire.py                         # Source acquisition (fs, s3, http, git, stream)
    parse/
      base.py
      capabilities.py
      ir.py
      planner.py
      quality.py
      registry.py
      providers/
        internal.py
        mineru.py                        # Adaptive retries + low-VRAM/page fallback strategy
        marker_mcp.py
        deepdoc.py
        docling.py
        transformers.py
    ocr/
      base.py
      heuristics.py
      planner.py
      registry.py
      providers/
        local.py
        external_service.py
        llm.py
    table/
      policy.py
      renderers.py
      schema.py
    convert/
      base.py                          # Converter interface
      pandas_conv.py                   # Tabular sources (CSV, Excel, Parquet)
      deepdoc_conv.py                  # Document extraction
      mineru_conv.py                   # External document intelligence
    chunk/
      base.py                          # Chunker interface
      fixed.py                         # Fixed token/window chunking
      recursive.py                     # Recursive splitter
      semantic.py                      # Semantic chunking (optional)
      boundary.py                      # Do-not-split marker helpers
    embed.py                           # Embedding integration (calls cloud_dog_llm)
    checkpoints.py                     # Checkpoint management for resumable ingestion
    verify.py                          # Read-back verification
  search/
    engine.py                          # Query planning: vector/hybrid/filters
    rerank.py                          # Optional reranking hooks
  embeddings/
    base.py                            # Embedding provider protocol
    providers.py                       # Ollama/OpenAI-compatible embedding providers
  jobs/
    models.py                          # Job model
    queue.py                           # Queue interface + basic operations
    worker.py                          # Optional worker loop
    status.py                          # Job status tracking
  collections/
    manager.py                         # Collection CRUD
    specs.py                           # Collection specification handling
  runtime/
    client.py                          # Runtime client implementation
    factory.py                         # Runtime client factory
  integrations/
    llamaindex.py                      # Optional LlamaIndex adapter
    langchain.py                       # Optional LangChain adapter
  factory.py                           # Public get_vdb_client entry integration
  compat/
    response_normaliser.py             # Legacy response schema normaliser (FR1.34)
  remote/
    client.py                          # Client-only remote proxy (FR1.35)
  versioning/
    schema_version.py                  # Collection schema versioning (FR1.36)
  observability/
    metrics.py                         # Metrics hooks
    otel.py                            # OpenTelemetry tracing
    audit.py                           # Audit event helpers
  testing/
    conformance.py                     # Cross-backend conformance test suite
    fixtures.py                        # Shared test fixtures
    mock_adapters.py                   # Mock backend adapters
```

---

## SA2 — Component Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                     Service (FastAPI / Agent / CLI)                   │
│                                                                      │
│  VDBClient.search() / .upsert_records() / .ingest()                 │
│         │                                                            │
│         ▼                                                            │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │                  VDBClient (main entry)                   │       │
│  │                                                           │       │
│  │  metadata/schema.py ──→ validate canonical fields         │       │
│  │  metadata/identity.py ──→ compute doc_id, record_id       │       │
│  │  capabilities/planner.py ──→ plan query strategy           │       │
│  │         │                                                 │       │
│  │         ▼                                                 │       │
│  │  ┌─────────────────────────────────────────────────┐     │       │
│  │  │           adapters/registry.py                   │     │       │
│  │  │  Active backends: [chroma, qdrant, opensearch]   │     │       │
│  │  │         │                                        │     │       │
│  │  │         ├──→ chroma.py ──→ chromadb              │     │       │
│  │  │         ├──→ qdrant.py ──→ qdrant-client         │     │       │
│  │  │         ├──→ weaviate.py ──→ weaviate-client     │     │       │
│  │  │         ├──→ opensearch.py ──→ opensearch-py     │     │       │
│  │  │         └──→ pgvector.py ──→ asyncpg+pgvector    │     │       │
│  │  └─────────────────────────────────────────────────┘     │       │
│  │         │                                                 │       │
│  │  options/manager.py ──→ merge common + backend-specific   │       │
│  │  (credentials pre-resolved by cloud_dog_config — PS-80)   │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │            ingestion/pipeline.py                          │       │
│  │                                                           │       │
│  │  1. acquire.py ──→ fetch source (fs/s3/http/git/stream)  │       │
│  │  2. convert/ ──→ pandas / deepdoc / mineru / generic      │       │
│  │  3. chunk/ ──→ fixed / recursive / semantic               │       │
│  │  4. embed.py ──→ cloud_dog_llm.embeddings                │       │
│  │  5. adapters/ ──→ upsert_records (via VDBClient)          │       │
│  │  6. verify.py ──→ optional read-back sampling             │       │
│  │  7. lifecycle/ ──→ supersession/archival                  │       │
│  │                                                           │       │
│  │  checkpoints.py ──→ resumable progress                    │       │
│  │  jobs/queue.py ──→ submit / track / cancel                │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │ lifecycle/  │  │ isolation/  │  │ access/     │                 │
│  │ manager.py  │  │ manager.py  │  │ policy.py   │                 │
│  │ retention   │  │ tenant iso  │  │ enforcement │                 │
│  └─────────────┘  └─────────────┘  └─────────────┘                 │
│                                                                      │
│  ┌─────────────────────────────────────────────────┐                │
│  │            observability/                        │                │
│  │  audit.py ← metrics.py ← otel.py                │                │
│  └─────────────────────────────────────────────────┘                │
└──────────────────────────────────────────────────────────────────────┘
```

---

## CC1 — Core Components

### CC1.1 VDBClient (Main Entry Point)

```python
class VDBClient:
    def __init__(self, config, adapter_registry): ...
    
    # Collection management
    async def init_backend(self, profile: str) -> bool: ...
    async def create_collection(self, spec: CollectionSpec) -> None: ...
    async def list_collections(self) -> list[CollectionInfo]: ...
    
    # CRUD
    async def upsert_records(self, collection: str, records: list[Record]) -> list[str]: ...
    async def get_record(self, collection: str, record_id: str) -> Record: ...
    async def search(self, collection: str, request: SearchRequest) -> SearchResponse: ...
    async def search_stream(self, collection: str, request: SearchRequest) -> AsyncIterator[SearchResult]: ...
    async def delete_record(self, collection: str, record_id: str) -> bool: ...
    async def purge(self, collection: str, filter_or_ids) -> int: ...
    
    # Capabilities
    def capabilities(self) -> CapabilityDescriptor: ...
```

### CC1.2 VDB Adapter Interface (`adapters/base.py`)

```python
class VDBAdapter(ABC):
    @abstractmethod
    async def initialize(self, config: dict) -> bool: ...
    @abstractmethod
    async def add_documents(self, collection, documents, metadatas?, ids?) -> list[str]: ...
    @abstractmethod
    async def search(self, collection, query, n_results, filter?, search_options?) -> list[dict]: ...
    @abstractmethod
    async def delete_document(self, collection, doc_id) -> bool: ...
    @abstractmethod
    async def update_document(self, collection, doc_id, content, metadata?) -> bool: ...
    @abstractmethod
    async def count_documents(self, collection, filter?) -> int: ...
    @abstractmethod
    async def health_check(self) -> bool: ...
    def capabilities(self) -> CapabilityDescriptor: ...
```

Compatible with existing `VectorStoreProvider` ABC from expert-agent.

### CC1.3 Options Manager (`options/manager.py`)

```python
class VectorStoreOptionsManager:
    @staticmethod
    def get_common_indexing_options(store_type, config) -> CommonIndexingOptions: ...
    @staticmethod
    def get_common_search_options(store_type, config, overrides?) -> CommonSearchOptions: ...
    @staticmethod
    def get_product_specific_options(store_type, config, overrides?) -> dict: ...
    @staticmethod
    def merge_options(store_type, config, overrides?) -> dict: ...
```

Compatible with existing `VectorStoreOptionsManager`.

### CC1.4 Metadata Validator (`metadata/schema.py`)

```python
class MetadataValidator:
    def validate(self, record: dict) -> ValidationResult: ...
    def compute_ids(self, record: dict) -> dict: ...
    def normalise_source_uri(self, uri: str) -> str: ...
```

### CC1.5 Lifecycle Manager (`lifecycle/manager.py`)

```python
class LifecycleManager:
    async def mark_deleted(self, collection, record_id) -> bool: ...
    async def mark_superseded(self, collection, record_ids: list) -> bool: ...
    async def purge(self, collection, record_id, safety_checks=True) -> bool: ...
    async def apply_retention(self, collection, policy) -> int: ...
```

### CC1.6 Ingestion Pipeline (`ingestion/pipeline.py`)

```python
class IngestionPipeline:
    def __init__(self, converter, chunker, embedder, vdb_client): ...
    
    async def ingest(self, source_spec, pipeline_spec, job_id?) -> IngestionResult: ...
    async def ingest_stream(self, source_stream, pipeline_spec, job_id?) -> AsyncIterator[ProgressEvent]: ...
```

Stages: acquire → convert → chunk → embed → upsert → verify → lifecycle.
Each stage is pluggable and observable.

### CC1.7 Job Queue (`jobs/queue.py`)

```python
class JobQueue:
    async def submit(self, job_request) -> str: ...
    async def get(self, job_id) -> Job: ...
    async def list(self, filters?, paging?) -> list[Job]: ...
    async def cancel(self, job_id) -> bool: ...
```

### CC1.8 Capability-Aware Planner (`capabilities/planner.py`)

```python
class QueryPlanner:
    def plan_search(self, request: SearchRequest, capabilities: CapabilityDescriptor) -> QueryPlan: ...
    def plan_delete(self, filter: dict, capabilities: CapabilityDescriptor) -> DeletePlan: ...
```

Uses capability descriptors to choose: vector-only vs hybrid, filter-before-search vs filter-after, native delete-by-filter vs emulated.

### CC1.9 Config Delegation

This package does NOT implement its own secret/Vault resolution. All credential fields in `ProviderConfig` (api_key, password, token, database_uri, etc.) arrive pre-resolved via `cloud_dog_config` (PS-80) variable substitution (e.g. `${vault.vdbs.chroma.api_key}`). Adapters receive fully-populated typed config objects and MUST NOT read `os.environ`, parse Vault JSON, or import any Vault client.

---

## DM1 — Data Model

### Persistent (via SQLAlchemy, optional)

| Table | Purpose | Key fields |
|-------|---------|-----------|
| `vector_stores` | Store configurations | id, name, type, config_json, enabled, access_control_json |
| `jobs` | Ingestion/admin job tracking | job_id, job_type, status, collection, progress, timestamps |

### In-Memory / Transient

| Object | Purpose |
|--------|---------|
| `CollectionSpec` | Collection creation specification |
| `Record` | Canonical record with metadata |
| `SearchRequest` / `SearchResponse` | Query and result objects |
| `Job` | Job state |
| `CapabilityDescriptor` | Backend capabilities |
| `CommonIndexingOptions` | Portable indexing config |
| `CommonSearchOptions` | Portable search config |

---

## DP1 — Dependency Policy

| Dependency | Status | Notes |
|-----------|--------|-------|
| `sqlalchemy` | Optional | Store config persistence (if using DB-backed store registry) |
| `httpx` | Optional | Remote backend connectivity |
| `chromadb` | Optional | Chroma backend |
| `qdrant-client` | Optional | Qdrant backend |
| `weaviate-client` | Optional | Weaviate backend |
| `opensearch-py` | Optional | OpenSearch backend |
| `asyncpg` + `pgvector` | Optional | PGVector backend |
| `pandas` | Optional | Tabular converter |
| `jinja2` | Optional | Template-based metadata |

All backend dependencies are optional — only required backends need to be installed.

---

## SE1 — Security Architecture

- Secrets (API keys, passwords) NEVER stored in persisted config. Credential resolution handled exclusively by `cloud_dog_config` (PS-80) — no Vault/env reads in this package.
- Secrets NEVER logged.
- Admin operations (collection delete, purge) require RBAC enforcement.
- Tenant isolation in all queries via `tenant_id`/`namespace` constraints.
- Access control policy enforced at service layer.

---

## Integration Pattern

```python
from cloud_dog_vdb import VDBClient, get_vdb_client
from cloud_dog_vdb.ingestion import IngestionPipeline
from cloud_dog_vdb.ingestion.chunk import RecursiveChunker
from cloud_dog_vdb.ingestion.convert import PandasConverter

# At startup
vdb = get_vdb_client(config)
await vdb.init_backend("production")

# Search
results = await vdb.search("my_collection", SearchRequest(
    query_text="example query",
    top_k=10,
    filters={"tenant_id": "t1"},
))

# Ingestion
pipeline = IngestionPipeline(
    converter=PandasConverter(),
    chunker=RecursiveChunker(chunk_size=512, overlap=50),
    embedder=embedding_manager,
    vdb_client=vdb,
)
result = await pipeline.ingest(
    source_spec={"uri": "s3://bucket/data.csv", "type": "file"},
    pipeline_spec={"collection": "my_collection", "namespace": "prod"},
)
```

---

## 0.4.0 Architecture Uplift (Implemented, Additive, Non-Breaking)

**Status:** Implemented  
**Date:** 2026-02-28  
**Reference:** `RELEASE_UPLIFT_PROPOSAL.md`

### SA3 — Additive Module Extensions

```text
cloud_dog_vdb/
  adapters/
    infinity.py
  ingestion/
    parse/
      base.py
      capabilities.py
      registry.py
      planner.py
      quality.py
      ir.py
      normalise.py
      providers/
        internal.py
        mineru.py
        deepdoc.py
        docling.py
        marker_mcp.py
        transformers.py
        pandoc.py
    ocr/
      base.py
      registry.py
      planner.py
      heuristics.py
      providers/
        local.py
        external_service.py
        llm.py
    table/
      policy.py
      renderers.py
      schema.py
    chunk/
      boundary.py
  testing/
    parser_corpus.py
    parser_benchmarks.py
  compat/
    ingestion_v1.py
```

Notes:
- Existing module paths remain valid.
- Existing `ingestion/convert/*` path remains supported by default.
- New modules are opt-in via config.

### SA4 — Ingestion Runtime Flows

#### SA4.1 Default Compatibility Flow
`acquire -> convert -> chunk -> embed -> upsert`

#### SA4.2 Extended Parser-First Flow
`acquire -> parser chain -> OCR (optional) -> Document IR -> table policy -> chunk -> embed -> upsert -> verify -> lifecycle`

### CC2 — New Core Responsibilities

#### CC2.1 Parser Registry and Planner
- deterministic parser chain resolution,
- capability checks,
- fallback policy execution with explicit reason capture.

#### CC2.2 OCR Planner and Provider Registry
- applies `disabled|auto|force` policy,
- runs heuristics in `auto`,
- attaches OCR provenance and quality/cost metadata.

#### CC2.3 Document IR
- canonical representation between parse and chunk stages,
- carries block/table structure, provenance, and quality signals.

#### CC2.4 Table Policy Engine
- converts table data according to policy (`text|markdown|html|json|dual`),
- validates JSON schema mode (`rows_cols|records`),
- emits table-aware chunks with provenance.

#### CC2.5 Compatibility Bridge (`compat/ingestion_v1.py`)
- adapts legacy converter path into the new internal execution model when needed,
- preserves 0.3.x defaults unless explicit uplift options are enabled.

#### CC2.6 Infinity Adapter (Proposed)
- `adapters/infinity.py` provides the Infinity VDB implementation behind the existing adapter contract,
- capabilities are advertised through existing capability descriptor APIs,
- config is loaded via pre-resolved `ProviderConfig` fields from `dev.vdbs.infinity`.

#### CC2.7 Corpus and Benchmark Harness
- `testing/parser_corpus.py` maps known files in `test-data/` to required invariants,
- `testing/parser_benchmarks.py` records parser/OCR performance metrics (`p50`, `p95`, throughput, quality pass rate),
- harness supports per-provider comparisons (MinerU, marker-mcp, internal parser).

### SE2 — External Provider Security Boundaries

External parser/OCR integrations MUST enforce:
- command allowlists for command-based providers,
- timeout and bounded input-size limits,
- isolated temp workspace usage,
- endpoint allowlists for HTTP/MCP providers,
- secret redaction in logs and audit records.

External service endpoints for MinerU and marker-mcp are resolved from config sourced via env files and/or Vault (`dev.services.*`) through `cloud_dog_config`; the package never resolves Vault directly.

### MU1 — Rollout Strategy

1. Ship parser/OCR framework disabled by default (compatibility-first release).
2. Enable parser chain per collection/profile in controlled rollout.
3. Keep explicit legacy opt-out path until formal deprecation decision.

---

## 0.5.0 Architecture Uplift — Marker MCP, Async Parse, Cross-Provider Comparison

**Status:** Implemented  
**Date:** 2026-03-04  
**Reference:** REQUIREMENTS.md § 0.5.0

### SA5 — Additive Module Extensions (0.5.0)

```text
cloud_dog_vdb/
  ingestion/
    parse/
      async_runner.py              # Async submit-poll-retrieve pattern (FR3.4)
      providers/
        marker_mcp.py              # Updated: output key fix, image extraction, TOC, async mode
  testing/
    comparison.py                  # Cross-provider comparison framework (FR3.6)
    comparison_report.py           # Report generation (JSON + markdown)
```

Notes:
- All new modules are additive — existing module paths remain valid.
- `async_runner.py` provides a reusable async wrapper that any HTTP-based parser provider can use.
- `comparison.py` is test infrastructure only — not imported by production code.

### SA6 — Async Parse Flow

```
┌────────────────────────────────────────────────────────────┐
│          Sync Path (default, ≤0.4.x behaviour)              │
│  parse_bytes(doc) ──▶ HTTP POST ──▶ wait ──▶ DocumentIR   │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│          Async Path (opt-in via async_mode=True)             │
│                                                              │
│  1. Submit: POST /marker/upload ──▶ job_id/request_id       │
│  2. Poll:  GET /marker/status/{id} ──▶ {status, progress}  │
│           │  ──▶ yield ProgressEvent to job queue              │
│           │  ──▶ sleep(poll_interval)                         │
│           └──▶ repeat until status=complete or timeout        │
│  3. Retrieve: parse response ──▶ DocumentIR                  │
│                                                              │
│  Fallback: if provider has no async API, use long-timeout    │
│            sync with heartbeat progress events.               │
└────────────────────────────────────────────────────────────┘
```

### CC3 — New Core Responsibilities (0.5.0)

#### CC3.1 AsyncParseRunner (`ingestion/parse/async_runner.py`)

```python
class AsyncParseRunner:
    """Reusable async submit-poll-retrieve for any HTTP parser provider."""
    def __init__(self, client: httpx.AsyncClient, config: AsyncParseConfig): ...
    async def run(
        self, submit_url: str, document: bytes, filename: str, **kwargs
    ) -> dict:
        """Submit, poll, retrieve. Yields ProgressEvents via callback."""
        ...
```

- Wraps the submit → poll → retrieve lifecycle.
- Configurable poll interval, max wait, timeout.
- Progress callback for integration with `cloud_dog_jobs` queue.
- Used by `MarkerMcpParserProvider` and `MinerUParserProvider` when async mode enabled.

#### CC3.2 MarkerMcpParserProvider (updated)

- Fix `_coerce_marker_text()` to check `output` key first (matches live Marker API).
- Extract `images` dict into `DocumentIR.artefact_refs`.
- Extract `metadata.table_of_contents` into heading blocks.
- Add `async_mode` support via `AsyncParseRunner`.
- Add `page_range` option for selective page extraction.

#### CC3.3 CrossProviderComparison (`testing/comparison.py`)

```python
class CrossProviderComparison:
    """Test-time facility: run same PDF through multiple providers, compare results."""
    async def compare(
        self, pdf_bytes: bytes, filename: str, providers: list[str],
        corpus_entry: dict | None = None,
    ) -> ComparisonReport: ...
```

- Per-provider: text_chars, heading_count, table_count, image_count, parse_time_ms, quality_pass_rate.
- Produces JSON + markdown reports.
- Handles provider unavailability (skip with reason, not fail).
- Records sync vs async mode per provider.

### MU2 — 0.5.0 Rollout Strategy

1. Fix `_coerce_marker_text()` bug first (non-breaking, critical).
2. Add image/TOC extraction to Marker provider (additive, non-breaking).
3. Add `async_runner.py` (additive, opt-in).
4. Wire async mode into Marker + MinerU providers.
5. Add comparison framework to `testing/`.
6. Enable `MARKER_MCP_ENABLED=true` in test env files.
7. Run full corpus comparison: Marker vs MinerU vs DeepDoc vs Docling.
8. Publish 0.5.0 to private PyPI.

## Auto-Declared Source Modules (Traceability Scanner)
<!-- TRACEABILITY-MODULE-LIST:START -->
The list below is generated from the current source tree and kept in sync for architecture-traceability audits.
- `cloud_dog_vdb/__init__.py`
- `cloud_dog_vdb/access/__init__.py`
- `cloud_dog_vdb/access/enforcement.py`
- `cloud_dog_vdb/access/policy.py`
- `cloud_dog_vdb/adapters/__init__.py`
- `cloud_dog_vdb/adapters/base.py`
- `cloud_dog_vdb/adapters/chroma.py`
- `cloud_dog_vdb/adapters/factory.py`
- `cloud_dog_vdb/adapters/infinity.py`
- `cloud_dog_vdb/adapters/opensearch.py`
- `cloud_dog_vdb/adapters/pgvector.py`
- `cloud_dog_vdb/adapters/qdrant.py`
- `cloud_dog_vdb/adapters/registry.py`
- `cloud_dog_vdb/adapters/vector_utils.py`
- `cloud_dog_vdb/adapters/weaviate.py`
- `cloud_dog_vdb/capabilities/__init__.py`
- `cloud_dog_vdb/capabilities/models.py`
- `cloud_dog_vdb/capabilities/planner.py`
- `cloud_dog_vdb/collections/__init__.py`
- `cloud_dog_vdb/collections/manager.py`
- `cloud_dog_vdb/collections/specs.py`
- `cloud_dog_vdb/compat/__init__.py`
- `cloud_dog_vdb/compat/response_normaliser.py`
- `cloud_dog_vdb/config/__init__.py`
- `cloud_dog_vdb/config/models.py`
- `cloud_dog_vdb/domain/__init__.py`
- `cloud_dog_vdb/domain/enums.py`
- `cloud_dog_vdb/domain/errors.py`
- `cloud_dog_vdb/domain/models.py`
- `cloud_dog_vdb/embeddings/__init__.py`
- `cloud_dog_vdb/embeddings/base.py`
- `cloud_dog_vdb/embeddings/providers.py`
- `cloud_dog_vdb/factory.py`
- `cloud_dog_vdb/ingestion/__init__.py`
- `cloud_dog_vdb/ingestion/acquire.py`
- `cloud_dog_vdb/ingestion/checkpoints.py`
- `cloud_dog_vdb/ingestion/chunk/__init__.py`
- `cloud_dog_vdb/ingestion/chunk/base.py`
- `cloud_dog_vdb/ingestion/chunk/boundary.py`
- `cloud_dog_vdb/ingestion/chunk/fixed.py`
- `cloud_dog_vdb/ingestion/chunk/recursive.py`
- `cloud_dog_vdb/ingestion/chunk/semantic.py`
- `cloud_dog_vdb/ingestion/convert/__init__.py`
- `cloud_dog_vdb/ingestion/convert/base.py`
- `cloud_dog_vdb/ingestion/convert/deepdoc_conv.py`
- `cloud_dog_vdb/ingestion/convert/mineru_conv.py`
- `cloud_dog_vdb/ingestion/convert/pandas_conv.py`
- `cloud_dog_vdb/ingestion/embed.py`
- `cloud_dog_vdb/ingestion/ocr/__init__.py`
- `cloud_dog_vdb/ingestion/ocr/base.py`
- `cloud_dog_vdb/ingestion/ocr/heuristics.py`
- `cloud_dog_vdb/ingestion/ocr/planner.py`
- `cloud_dog_vdb/ingestion/ocr/providers/__init__.py`
- `cloud_dog_vdb/ingestion/ocr/providers/external_service.py`
- `cloud_dog_vdb/ingestion/ocr/providers/llm.py`
- `cloud_dog_vdb/ingestion/ocr/providers/local.py`
- `cloud_dog_vdb/ingestion/ocr/registry.py`
- `cloud_dog_vdb/ingestion/parse/__init__.py`
- `cloud_dog_vdb/ingestion/parse/async_runner.py`
- `cloud_dog_vdb/ingestion/parse/base.py`
- `cloud_dog_vdb/ingestion/parse/capabilities.py`
- `cloud_dog_vdb/ingestion/parse/ir.py`
- `cloud_dog_vdb/ingestion/parse/planner.py`
- `cloud_dog_vdb/ingestion/parse/providers/__init__.py`
- `cloud_dog_vdb/ingestion/parse/providers/deepdoc.py`
- `cloud_dog_vdb/ingestion/parse/providers/docling.py`
- `cloud_dog_vdb/ingestion/parse/providers/internal.py`
- `cloud_dog_vdb/ingestion/parse/providers/marker_mcp.py`
- `cloud_dog_vdb/ingestion/parse/providers/mineru.py`
- `cloud_dog_vdb/ingestion/parse/providers/transformers.py`
- `cloud_dog_vdb/ingestion/parse/quality.py`
- `cloud_dog_vdb/ingestion/parse/registry.py`
- `cloud_dog_vdb/ingestion/pipeline.py`
- `cloud_dog_vdb/ingestion/table/__init__.py`
- `cloud_dog_vdb/ingestion/table/policy.py`
- `cloud_dog_vdb/ingestion/table/renderers.py`
- `cloud_dog_vdb/ingestion/table/schema.py`
- `cloud_dog_vdb/ingestion/verify.py`
- `cloud_dog_vdb/integrations/__init__.py`
- `cloud_dog_vdb/integrations/langchain.py`
- `cloud_dog_vdb/integrations/llamaindex.py`
- `cloud_dog_vdb/isolation/__init__.py`
- `cloud_dog_vdb/isolation/manager.py`
- `cloud_dog_vdb/jobs/__init__.py`
- `cloud_dog_vdb/jobs/models.py`
- `cloud_dog_vdb/jobs/queue.py`
- `cloud_dog_vdb/jobs/status.py`
- `cloud_dog_vdb/jobs/worker.py`
- `cloud_dog_vdb/lifecycle/__init__.py`
- `cloud_dog_vdb/lifecycle/manager.py`
- `cloud_dog_vdb/lifecycle/retention.py`
- `cloud_dog_vdb/metadata/__init__.py`
- `cloud_dog_vdb/metadata/identity.py`
- `cloud_dog_vdb/metadata/normalise.py`
- `cloud_dog_vdb/metadata/schema.py`
- `cloud_dog_vdb/observability/__init__.py`
- `cloud_dog_vdb/observability/audit.py`
- `cloud_dog_vdb/observability/metrics.py`
- `cloud_dog_vdb/observability/otel.py`
- `cloud_dog_vdb/options/__init__.py`
- `cloud_dog_vdb/options/chroma.py`
- `cloud_dog_vdb/options/common.py`
- `cloud_dog_vdb/options/manager.py`
- `cloud_dog_vdb/options/opensearch.py`
- `cloud_dog_vdb/options/pgvector.py`
- `cloud_dog_vdb/options/qdrant.py`
- `cloud_dog_vdb/options/weaviate.py`
- `cloud_dog_vdb/remote/__init__.py`
- `cloud_dog_vdb/remote/client.py`
- `cloud_dog_vdb/runtime/__init__.py`
- `cloud_dog_vdb/runtime/client.py`
- `cloud_dog_vdb/runtime/factory.py`
- `cloud_dog_vdb/search/__init__.py`
- `cloud_dog_vdb/search/engine.py`
- `cloud_dog_vdb/search/rerank.py`
- `cloud_dog_vdb/testing/__init__.py`
- `cloud_dog_vdb/testing/comparison.py`
- `cloud_dog_vdb/testing/comparison_report.py`
- `cloud_dog_vdb/testing/conformance.py`
- `cloud_dog_vdb/testing/fixtures.py`
- `cloud_dog_vdb/testing/mock_adapters.py`
- `cloud_dog_vdb/versioning/__init__.py`
- `cloud_dog_vdb/versioning/schema_version.py`
<!-- TRACEABILITY-MODULE-LIST:END -->
