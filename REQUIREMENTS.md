# platform-vdb — Requirements

**Package:** `cloud_dog_vdb`  
**Version:** 0.5.0  
**Standard:** PS-60 (Vector DB Interfaces)  
**Status:** Active

---

## Scope / Vision

### SV1.1
The package SHALL provide a single, reusable vector database interface library for all Cloud-Dog Python services, implementing PS-60.

### SV1.2
The package SHALL support pluggable backend adapters, canonical metadata enforcement, deterministic identity, ingestion pipelines, job control, capability-aware query planning, and lifecycle management — all behind stable interfaces.

### SV1.3
The package SHALL be compatible with the existing expert-agent VDB implementation (5 backend providers, options management, lifecycle, isolation, indexing managers) and provide a clean migration path.

---

## Business Objectives

### BO1.1
Eliminate per-project VDB adapter reimplementation — centralise provider logic, metadata validation, and lifecycle management.

### BO1.2
Enable consistent ingestion pipelines across all services — same acquire → convert → chunk → embed → upsert flow.

### BO1.3
Support progressive adoption: services can start with basic search/add and add ingestion pipelines, job control, and advanced search incrementally.

---

## Functional Requirements

### FR1.1 — Backend Adapter Interface
The package MUST define an abstract `VDBAdapter` interface:
- `initialize(config) -> bool`
- `add_documents(collection, documents, metadatas?, ids?) -> list[str]`
- `search(collection, query, n_results, filter?, search_options?) -> list[dict]`
- `delete_document(collection, doc_id) -> bool`
- `update_document(collection, doc_id, content, metadata?) -> bool`
- `count_documents(collection, filter?) -> int`
- `health_check() -> bool`
- `capabilities() -> CapabilityDescriptor`

### FR1.2 — Chroma Adapter
The package MUST provide a Chroma adapter supporting:
- Local (PersistentClient) and remote (HttpClient) modes.
- HNSW collection options (`hnsw_space`, `hnsw_construction_ef`, `hnsw_M`, `hnsw_search_ef`, etc.).
- Metadata filtering (`where`, `where_document`).
- Result projection (`include`).

### FR1.3 — Qdrant Adapter
The package MUST provide a Qdrant adapter supporting:
- HNSW parameters, quantization (scalar/product/binary).
- Payload indexing, shard configuration.
- SSL/TLS with API key auth.
- Named vectors, sparse vectors (where supported).

### FR1.4 — Weaviate Adapter
The package MUST provide a Weaviate adapter supporting:
- v4 collections API (primary) with v3 legacy fallback.
- Self-provided vectors (vectorizer: none).
- Vector index types: HNSW, flat, dynamic.
- GraphQL and REST query paths.

### FR1.5 — OpenSearch Adapter
The package MUST provide an OpenSearch adapter supporting:
- kNN vector search with configurable space_type, method, engine.
- HNSW and NMSLIB engines.
- PQ/flat encoder configuration.
- Hybrid search (BM25 + vector) via bool queries.
- Username/password and API key auth.

### FR1.6 — PGVector Adapter
The package MUST provide a PGVector adapter supporting:
- PostgreSQL extension with IVFFlat and HNSW index types.
- Configurable distance operators (L2, cosine, inner product).
- Async via asyncpg.
- Query-time parameter setting (probes, ef_search).

### FR1.7 — Custom / Extension Adapters
Any additional backend MUST be pluggable via the adapter interface without changing the domain API.

### FR1.8 — Capability Descriptor
Each adapter MUST expose `capabilities()` returning: `filtering`, `hybrid_search`, `sparse_vectors`, `multi_vector`, `metadata_indexing`, `upsert_semantics`, `delete_by_filter`, `ttl_native`, `transactions`, `consistency`, `max_metadata_bytes`, `max_batch_size`, `supports_multimodal`.

### FR1.9 — Canonical Metadata Validation
The package MUST validate all records against the canonical metadata schema (PS-60 VDB1):
- Required fields enforced.
- Enum values validated (`source_type`, `lifecycle_state`).
- UTC RFC3339 timestamps enforced.
- Max metadata size enforced (spill to external store if exceeded).

### FR1.9A — Metadata-Pack Portable Filter Baseline
The package MUST preserve the archived metadata-pack management baseline as a portable adapter contract for all supported backends:
- `tenant_id`
- `dataset_id`
- `collection_id`
- `document_id`
- `chunk_id`
- `status`
- `language`
- `source_type`
- `authoritative_source`
- `updated_at`
- `ingested_at`
- `embedding_model`
- `index_version`

The package MUST expose those fields through `MetadataFilter`, `coerce_metadata_filter`, `filter_to_backend_query`, and `matches_metadata` so services can use the same filter contract for Qdrant, Chroma, OpenSearch, PGVector, and Weaviate. Tests MUST prove translation and matching for the full baseline.

### FR1.10 — Deterministic Identity
The package MUST compute stable IDs:
- `doc_id = sha256(tenant_id + "|" + normalised_source_uri)`
- `content_hash = sha256(normalised_embedding_text)`
- `record_id = sha256(doc_id + "|" + content_hash + "|" + chunk_id + "|" + embedding_model + "|" + chunker_version)`

### FR1.11 — Collection Management
The package MUST provide:
- `init_backend(profile)` — validate connectivity, create schema/collections/indexes.
- `create_collection(spec)`, `get_collection(name)`, `list_collections()`, `update_collection()`, `delete_collection()` (admin-only).
- Collection spec: name, namespace, embedding_dim, distance_metric, metadata_schema, index_params, access_policy.

### FR1.12 — CRUD Operations
The package MUST provide:
- `upsert_records(records[])` — validates metadata, computes IDs/hashes, applies supersession.
- `get_record(record_id)`, `list_records(filters, paging)`.
- `update_record(record_id, patch)`.
- `delete_record(record_id)` — soft-delete.
- `delete_by_filter(filter)` — if supported; emulate otherwise.
- `count_documents(collection, filter?)`.

### FR1.13 — Search Operations
The package MUST provide:
- `search(collection, request) -> SearchResponse` — vector similarity + metadata filtering.
- `search_stream(collection, request) -> AsyncIterator[SearchResult]` — streaming where feasible.
- Portable `SearchRequest`: collection, query_text/query_vector, top_k, filters (portable + x_backend.*), include_metadata/vectors, query_plan hints, score_threshold.

### FR1.14 — Common Search Options
The package MUST support (compatible with existing `CommonSearchOptions`):
- `limit`, `top_k`, `filter`, `pre_filter`, `offset`, `cursor`.
- `score_threshold`, `min_score`, `max_distance`.
- `include_vectors`, `include_metadata`, `include_distances`, `include_scores`.
- `hybrid_enabled`, `hybrid_alpha`.

### FR1.15 — Common Indexing Options
The package MUST support (compatible with existing `CommonIndexingOptions`):
- `dimension`, `distance_metric` (l2/cosine/dot/ip).
- `index_type` (exact/ann/hnsw/ivfflat/flat).
- `ann_algorithm`, `compression_enabled`, `quantization_type`.
- `shards`, `replicas`, `persistence`.

### FR1.16 — Backend-Specific Options
The package MUST support backend-specific options under `x_backend.<name>.*`:
- Chroma HNSW params.
- Qdrant HNSW, quantization, payload indexing.
- Weaviate class settings, module config.
- OpenSearch space_type, method, engine, encoder.
- PGVector index_type, IVFFlat lists, HNSW m/ef_construction.

Compatible with existing `VectorStoreOptionsManager`.

### FR1.17 — Lifecycle Management
The package MUST support:
- Soft-delete first (mark lifecycle_state=deleted, is_latest=false).
- Purge via admin/retention job with safety checks.
- Supersession: previous latest → superseded, new → active + is_latest.
- TTL/retention policies per collection and per record.

### FR1.18 — Access Controls
The package MUST support:
- Read/write/admin roles per collection.
- Tenant/namespace isolation.
- Optional record-level access_tags.
- Backend enforcement where supported; service-layer pre-checks elsewhere.

### FR1.19 — Ingestion Pipeline
The package MUST provide a standard pipeline: acquire → convert → chunk → embed → upsert → verify → lifecycle.

### FR1.20 — Pluggable Converters
The package MUST support pluggable conversion backends:
- pandas (tabular), deepdoc, docling, mineru, transformers, generic external.
- Converters emit: normalised text, structured blocks, extracted metadata, artefact references.
- External parser providers MUST implement bounded retry handling for transient transport/5xx errors; MinerU MUST support optional low-VRAM and page-sliced fallback controls for PDF parsing under resource pressure.

### FR1.21 — Pluggable Chunkers
The package MUST support:
- Fixed token/window chunking.
- Recursive splitter.
- Semantic chunking (optional).
- Overlap controls, per-document overrides.
- Chunker params recorded in metadata.

### FR1.22 — Embedding Integration
- Embedding generation MUST use `cloud_dog_llm` embedding interface.
- Dimension validation against collection specs.
- Batching, retries, rate limiting, timeouts.

### FR1.23 — Streaming Ingestion
For large inputs:
- Streaming file reads / incremental conversion.
- Incremental chunking + embedding batches.
- Progressive upsert with checkpointing.
- Job progress updates.

### FR1.24 — Job Control
The package MUST provide:
- Job model: `job_id`, `job_type`, `status`, `priority`, `collection`, `tenant_id`, `input_spec`, `progress`, `timestamps`, `error`, `correlation_id`.
- Queue operations: `submit_job`, `get_job`, `list_jobs`, `cancel_job`, optional `pause_job`/`resume_job`.
- Checkpoints for resumable ingestion.
- Deterministic record_id prevents duplicates on retry.

### FR1.25 — Hybrid Search
Where backend supports: BM25 + vector hybrid search with configurable alpha weighting.
Emulation optional where not supported.

### FR1.26 — Reranking Hooks
Optional reranking via LLM module or local reranker.

### FR1.27 — Multimodal Support
If backend + embedding stack support image/audio: store multimodal vectors, query by modality. Otherwise degrade gracefully.

### FR1.28 — Framework Integration (Optional)
Optional adapters for LlamaIndex and LangChain vectorstore interfaces. NOT required dependencies.

### FR1.29 — Observability
- Response metadata: request_id, namespace, query_plan.
- Metrics: jobs, throughput, embedding latency, upsert latency, query latency.
- Audit hooks for admin/destructive actions.
- OpenTelemetry tracing hooks.

### FR1.30 — Configuration via Platform Config
All settings via `cloud_dog_config` (PS-80): `vector_stores.*`, `vector_stores_config.*`, `embeddings.*`.

### FR1.30a — Vault Config Sections for VDB
IT and AT tests MUST obtain VDB backend credentials from Vault (PS-80 CM9). The following Vault config sections are required:

**VDB Backends:**

| Vault Path | Content | Used For |
|------------|---------|----------|
| `dev.vdbs.chroma` | `base_url`, `auth_token`, `port`, `ssl` | Chroma adapter IT tests (IT1.1-IT1.2) |
| `dev.vdbs.qdrant` | `host`, `port`, `url`, `api_key` | Qdrant adapter IT tests (IT1.3-IT1.4) |
| `dev.vdbs.weaviate` | `url`, `api_key`, `username`, `port` | Weaviate adapter IT tests (IT1.5-IT1.6) |
| `dev.vdbs.opensearch` | `host`, `port`, `username`, `password`, `ssl` | OpenSearch adapter IT tests (IT1.7-IT1.8) |
| `dev.vdbs.pgvector` | `host`, `port`, `username`, `password`, `database`, `database_uri` | PGVector adapter IT tests (IT1.9-IT1.10) |
| `dev.vdbs.infinity` | `host`, `port`, `url`, `api_key`, `database`, `tls` | Proposed Infinity adapter IT/CT tests (IT2.x/CT1.x, 0.4.0 uplift) |

**External Parser/OCR Services (for parser integration tiers):**

| Vault Path | Content | Used For |
|------------|---------|----------|
| `dev.services.mineru` | `base_url`, `api_key`, `timeout_seconds`, `enabled` | MinerU parser integration tests (PT1.1) |
| `dev.services.marker_mcp` or `dev.services.markermcp` | `base_url`, `auth_token`, `timeout_seconds`, `enabled` | marker-mcp parser integration tests (PT1.4). **Service LIVE at `https://marker0.cloud-dog.net` as of 2026-03-04.** |

**Embedding Models (for ingestion and search IT tests):**

Ollama (self-hosted, no API key):

| Vault Path | Model | Dimensions | URL |
|------------|-------|-----------|-----|
| `dev.models.ollama_bge_m3_567m_llm1` | `bge-m3:567m` | 1024 | `https://llm1.cloud-dog.net` |
| `dev.models.ollama_bge_m3_567m_llm2` | `bge-m3:567m` | 1024 | `https://llm2.cloud-dog.net` |
| `dev.models.ollama_nomic_embed_text_llm1` | `nomic-embed-text:latest` | 768 | `https://llm1.cloud-dog.net` |
| `dev.models.ollama_nomic_embed_text_llm2` | `nomic-embed-text:latest` | 768 | `https://llm2.cloud-dog.net` |
| `dev.models.ollama_granite_embedding_278m_llm1` | `granite-embedding:278m` | 768 | `https://llm1.cloud-dog.net` |
| `dev.models.ollama_granite_embedding_278m_llm2` | `granite-embedding:278m` | 768 | `https://llm2.cloud-dog.net` |

OpenAI-compatible via OpenRouter (requires API key):

| Vault Path | Model | Dimensions | URL |
|------------|-------|-----------|-----|
| `dev.models.openai_text_embedding_baai_bge_m3_openrouter` | `baai/bge-m3` | 1024 | `https://openrouter.ai/api/v1` |
| `dev.models.openai_text_embedding_3_small_openrouter` | `openai/text-embedding-3-small` | 1536 | `https://openrouter.ai/api/v1` |
| `dev.models.openai_text_embedding_3_large_openrouter` | `openai/text-embedding-3-large` | 3072 | `https://openrouter.ai/api/v1` |
| `dev.models.openai_text_embedding_ada_002_openrouter` | `openai/text-embedding-ada-002` | 1536 | `https://openrouter.ai/api/v1` |

Recommended test combinations:
- **Primary (Ollama):** `ollama_bge_m3_567m_llm2` — 1024 dims, fast, no API key.
- **Secondary (Ollama):** `ollama_nomic_embed_text_llm1` — 768 dims, alternative model/host.
- **OpenAI-compat:** `openai_text_embedding_baai_bge_m3_openrouter` — same model via OpenRouter, validates provider abstraction.

Tests MUST be env-gated per backend: skip gracefully if the required Vault section is absent or the backend is unreachable.
UT/ST tests MUST use mock/in-memory adapters — no real backend calls.

### FR1.31 — Config Delegation (No Local Secret Resolution)
The package MUST NOT implement its own Vault/secret resolution logic. All backend credentials (API keys, passwords, tokens) MUST arrive pre-resolved via `cloud_dog_config` (PS-80) variable substitution (e.g. `${vault.vdbs.chroma.api_key}`). Adapters receive fully-resolved `ProviderConfig` objects — they MUST NOT read `os.environ`, parse Vault JSON, or navigate Vault path structures directly.

### FR1.32 — Conformance Test Suite
Cross-backend test suite verifying: adapter interfaces, capability descriptors, deterministic IDs, soft-delete + purge safety, pipeline correctness, streaming ingestion/search.

### FR1.33 — Config-Delegation Verification
The package MUST verify that no module reads `os.environ` directly, navigates Vault JSON structures, or implements its own secret merging. All credential fields in `ProviderConfig` (api_key, password, token, etc.) are populated by `cloud_dog_config` before the adapter receives them. Tests MUST confirm adapters work with pre-resolved config objects and do not import or reference any Vault client.
- **Source**: architecture principle — `cloud_dog_config` is the single config subsystem (PS-80 acceptance criteria).

### FR1.34 — Compatibility Normaliser
The package MUST provide a `ResponseNormaliser` for normalising legacy VDB response schemas:
- Translates backend-specific response formats (Chroma, Qdrant, Weaviate, OpenSearch, PGVector) to the unified `SearchResult` / `Record` models.
- Legacy response schemas accepted and normalised without data loss.
- Use case: expert-agent has bespoke VDB response parsing.
- **Source**: expert-agent (compatibility normaliser for legacy response schemas).

### FR1.35 — Client-Only Integration Mode
The package SHOULD support a lightweight client-only integration mode:
- No local VDB backend; connects to remote VDB service via HTTP/gRPC.
- `VDBClient(remote_url=...)` — proxy that delegates all operations to a remote service.
- Use case: chat-client needs VDB-backed retrieval but the VDB runs remotely.
- **Source**: chat-client (client-only integration guidance for VDB-backed remote services).

### FR1.36 — Collection Schema Versioning
The package SHOULD support collection schema versioning:
- Collections track schema version (dimension count, metadata fields, embedding model).
- Schema migration utility for dimension changes (re-embed required).
- Version mismatch detection at query time.
- **Source**: foresight (collection lifecycle management).

---

## Non-Functional Requirements

### NF1.1
Runtime deps: `sqlalchemy` (store config persistence), `httpx` (for remote backends). Backend-specific: `chromadb`, `qdrant-client`, `weaviate-client`, `opensearch-py`, `asyncpg`+`pgvector` — all optional.

### NF1.2
Search operations (excluding network I/O) MUST add < 5ms overhead.

### NF1.3
All public APIs MUST have async variants.

### NF1.4
Python 3.10+.

---

## Cyber Security

### CS1.1
Secrets (API keys, passwords) MUST NEVER be stored in persisted config. Credential resolution is handled exclusively by `cloud_dog_config` (PS-80) — no Vault/env reads in this package.

### CS1.2
Secrets MUST NEVER be logged.

### CS1.3
Admin operations (collection delete, purge) require RBAC enforcement.

### CS1.4
Tenant isolation enforced in all queries.

---

## Acceptance Criteria

A project is compliant when:
- All VDB operations go through `cloud_dog_vdb`.
- Collections created/validated via module initialisation.
- Ingestion uses the pipeline and records canonical metadata.
- Jobs submitted via queue interface with progress/status.
- Search supports portable filters and capability-aware planning.
- Deletes are soft-delete first; purge via retention/admin only.
- No direct `os.environ`, `hvac`, or Vault reads for credentials — all config via `cloud_dog_config` (PS-80).
- No `secrets/` module or secret overlay/resolver logic exists in the package.
- Conformance test suite passes for each configured backend.

---

## 0.4.0 Uplift (Implemented, Additive, Non-Breaking)

**Status:** Implemented  
**Date:** 2026-02-28  
**Reference:** `RELEASE_UPLIFT_PROPOSAL.md`

### Compatibility Contract (Non-Negotiable)

#### BC1 — API Compatibility
- Public Python APIs used by current consumers MUST remain stable.
- Existing method signatures MUST continue to work without caller changes.
- New functionality MUST be opt-in via additive config/options.

#### BC2 — Behavioural Compatibility by Default
- If no new parser/OCR options are set, ingestion behaviour MUST match 0.3.x defaults.
- Existing converter+chunker paths MUST continue to execute successfully.

#### BC3 — Configuration Compatibility
- Existing config keys MUST remain accepted.
- New keys MUST be additive.
- If aliases are introduced, deprecation warnings MUST be explicit.

#### BC4 — Metadata/Identity Compatibility
- Canonical metadata and deterministic ID rules remain unchanged.
- New provenance fields are additive and optional.

#### BC5 — Error/Envelope Compatibility
- Existing portable error taxonomy and response envelope remain stable.
- New parser/OCR errors map to existing top-level taxonomy categories.

### Functional Uplift

#### FR2.1 — Parser Provider Framework
The package MUST support parser providers in four modes:
- in-process Python provider,
- external command provider,
- external HTTP provider,
- external MCP provider.

Each provider exposes: `provider_id`, version, capability descriptor, health check, and parse entrypoint returning Document IR.

#### FR2.2 — Supported Parser Options
The package MUST provide adapters (or integration contracts) for:
- MinerU,
- marker-mcp,
- DeepDoc,
- Docling,
- baseline internal parser.

Pandoc integration SHOULD be supported where available.

#### FR2.3 — Parser Chain and Deterministic Fallback
A configurable ordered parser chain MUST be supported per profile/collection with deterministic fallback on:
- provider error,
- timeout,
- quality threshold miss,
- missing required capability (for example table extraction requirement).

When source bytes are ingested with `source_uri` metadata, the pipeline MUST infer filename/MIME hints from `source_uri` when the acquired filename is generic (for example `document.bin`) so parser capability checks and provider-specific PDF paths remain deterministic.

#### FR2.4 — Parser Capability Model
Capabilities MUST include at least:
- `supports_pdf`, `supports_docx`, `supports_html`,
- `supports_layout`, `supports_tables`, `supports_images`,
- `supports_ocr_passthrough`,
- `supports_streaming`,
- `max_document_bytes`.

#### FR2.5 — Document IR
The ingestion pipeline MUST support a Document IR between parsing and chunking, carrying:
- text/heading/list blocks,
- structured tables,
- optional artefact references,
- provenance fields,
- quality metrics.

#### FR2.6 — OCR Modes and Providers
OCR MUST be optional and policy-driven with modes:
- `disabled`, `auto`, `force`.

OCR providers MUST be pluggable:
- local engine,
- external OCR service,
- external LLM OCR.

#### FR2.7 — OCR Heuristics and Provenance
`auto` OCR mode MUST support configurable heuristics (text density, scanned-page ratio, parser confidence) and record decision outcomes in metadata/audit.

#### FR2.8 — OCR Cost/Safety Controls
LLM OCR integrations MUST support per-document and per-job cost caps, rate limiting, timeout/retry policy, and explicit profile/environment disable switches.

#### FR2.9 — Table Handling Policies
Table handling MUST support:
- `table_as_text`,
- `table_as_markdown`,
- `table_as_html`,
- `table_as_json`,
- `table_dual`.

For `table_as_json`, standard output shapes MUST include:
- `rows_cols`,
- `records`.

#### FR2.10 — Table Chunking Controls
Table chunking MUST support:
- `whole_table`,
- `row_chunks` with max rows per chunk.

#### FR2.11 — Chunking Controls Uplift
Chunking MUST support explicit controls for:
- unit: `tokens|characters|bytes`,
- chunk size and overlap,
- separator priorities,
- boundary source preference (`heading`, `paragraph`, `page`, `table_row`, `semantic`),
- do-not-split guards (tables, code blocks, heading+lead paragraph).

#### FR2.12 — Provenance Extensions
Chunks MUST support additive provenance fields including parser provider/version, OCR provider/version, page range, section path, table locator, and chunk kind.

#### FR2.13 — Capability-Aware Ingestion Planning
Ingestion planning MUST select parser/OCR/table/chunk strategy based on requested policy, provider capabilities, profile constraints, and source characteristics.

#### FR2.14 — Extensibility Contract
New parser providers MUST be registerable without breaking the core domain API or existing adapter interfaces.

#### FR2.15 — Config Delegation (Parser/OCR)
Parser/OCR endpoints and credentials MUST be pre-resolved by `cloud_dog_config`.  
This package MUST NOT import `hvac`, parse Vault JSON, or read credential env vars directly.

#### FR2.16 — Observability Extensions
Ingestion observability MUST include provider selection, fallback reason, OCR decisions, table/chunk policies, stage timings, and cost counters (where relevant).

#### FR2.17 — Documentation Uplift
0.4.0 release documentation MUST include parser capability matrices, OCR policy guide, table policy guide, and explicit non-breaking migration notes.

#### FR2.18 — Corpus-Driven Validation (`test-data/`)
Parser/OCR/table validation MUST use the committed corpus under `packages/backend/platform-vdb/test-data/`, including mixed, scanned, table-heavy, and handwritten PDFs.

The quality/performance test plan MUST map each corpus file to:
- parser chain profile,
- OCR mode,
- table policy,
- expected extraction invariants.

#### FR2.19 — Parser/OCR Performance Requirements
Performance suites MUST benchmark parser and OCR providers against the known corpus and record at minimum:
- parse latency (`p50`, `p95`),
- throughput (pages/sec or bytes/sec),
- OCR invocation ratio in `auto` mode,
- extraction quality pass/fail rate by invariant set.

#### FR2.20 — External Service Endpoint Resolution
MinerU and marker-mcp integration endpoints MUST be configurable via env files and/or Vault-backed config resolution through `cloud_dog_config` using `dev.services.*` sections.

Tests MUST validate both:
- env-file supplied endpoint mode,
- Vault-resolved endpoint mode.

Provider health checks for external parser services SHOULD tolerate transient endpoint variance (for example `openapi` probe instability) via bounded retries and alternate liveness probes, while still failing closed when no reachable endpoint is available.

#### FR2.21 — LLM Embedding Integration Validation
Parser-first ingestion tests MUST include embedding generation against configured embedding providers and validate:
- embedding dimension compatibility,
- stable upsert/search behaviour under parser/OCR variants.

#### FR2.22 — Infinity Backend Support (Proposed 0.4.0)
The package SHOULD add an Infinity VDB adapter with capability descriptor and portable API parity matching existing backends.

Infinity configuration MUST be sourced from `dev.vdbs.infinity` (via `cloud_dog_config`) and included in cross-backend conformance/compatibility suites.

---

### 0.5.0 Uplift — Marker MCP Enablement, Async Parser, Cross-Provider Comparison

**Status:** Implemented (service-dependent live gates)  
**Date:** 2026-03-04  
**Backwards Compatibility:** MANDATORY — all 0.4.x public APIs, config keys, and default behaviours MUST remain unchanged.

#### Compatibility Contract (0.5.0)

The same BC1–BC5 contract from 0.4.0 applies. Additionally:

- **BC6 — Existing parser providers remain default.** If no `async_mode` option is set, parser behaviour MUST match 0.4.x synchronous defaults.
- **BC7 — Comparison framework is opt-in.** Cross-provider comparison is a test/dev facility only — not invoked in production ingestion paths.

#### FR3.1 — Marker MCP Response Contract
The Marker MCP provider MUST correctly parse the live Marker API response envelope:
- `success: bool` — parse succeeded.
- `output: str` — extracted markdown text (primary content key).
- `format: str` — output format identifier (`"markdown"`).
- `images: dict[str, str]` — map of image reference keys to base64-encoded JPEG data.
- `metadata.table_of_contents: list[dict]` — TOC entries with title, level, page_id, polygon.
- `metadata.page_stats: list[dict]` — per-page block counts and extraction method.

The `_coerce_marker_text()` function MUST check the `output` key (the actual Marker API field) in addition to `markdown`, `text`, `content`.

#### FR3.2 — Marker MCP Image/Artefact Extraction
When the Marker response includes `images`, the provider MUST:
- Decode base64 image data.
- Store images as artefact references in the `DocumentIR` (via `artefact_refs` list).
- Preserve image reference keys so markdown `![](<key>)` links resolve.
- Record `images_count` in quality metadata.

#### FR3.3 — Marker MCP Table of Contents Extraction
When the Marker response includes `metadata.table_of_contents`, the provider MUST:
- Parse TOC entries into `DocumentIR` heading blocks with level, title, and page reference.
- Use TOC structure to improve heading detection in the text extraction.

#### FR3.4 — Async Parser Mode (Long-Running Extraction)
External parser providers (Marker MCP, MinerU) MUST support an async execution mode for documents that exceed a configurable timeout threshold:

- **Submit phase:** Upload document, receive `job_id` or `request_id`.
- **Poll phase:** Periodically poll for completion with configurable interval and max wait.
- **Retrieve phase:** Fetch completed result.
- **Timeout handling:** If max wait exceeded, raise `ParserTimeoutError` (mapped to existing error taxonomy).
- **Cancellation:** Best-effort cancellation via provider API if supported.

Async mode MUST be opt-in via `options={"async_mode": True}` or triggered automatically when `estimated_parse_time > async_threshold_seconds` (configurable, default 30s).

The async pattern MUST integrate with the existing `cloud_dog_jobs` queue so that:
- Long-running parse jobs appear in the job queue with progress updates.
- Ingestion pipelines can yield progress events during the poll phase.
- Checkpoint/resume works across async parse boundaries.

#### FR3.5 — Marker MCP Async Upload Support
The Marker MCP provider MUST support the async pattern from FR3.4:
- Upload endpoint: `POST /marker/upload` with `output_format=markdown`, `paginate_output=true`.
- The current synchronous path (wait for HTTP 200 response) MUST remain the default.
- When `async_mode=True` or document size exceeds threshold, use submit-poll-retrieve pattern if the Marker API supports it, or fall back to long-timeout synchronous with progress heartbeats.

#### FR3.6 — Cross-Provider Comparison Framework
The package MUST provide a test-time comparison framework that:

1. **Runs the same PDF through multiple parser providers** (Marker MCP, MinerU, DeepDoc, Docling, internal, Transformers) using the corpus manifest.
2. **Records per-provider extraction metrics:**
   - `text_chars`: total extracted text length.
   - `heading_count`: number of detected headings.
   - `table_count`: number of detected tables.
   - `image_count`: number of extracted images/artefacts.
   - `parse_time_ms`: wall-clock parse latency.
   - `quality_invariant_pass_rate`: percentage of corpus invariants met.
   - `chunk_count`: chunks produced after standard chunking.
3. **Produces a comparison report** (JSON + optional markdown) ranking providers by quality/speed trade-off per document category.
4. **Supports configurable provider subsets** — run comparison for any combination of available providers.
5. **Handles provider unavailability gracefully** — skip unavailable providers with explicit reason, do not fail the entire comparison run.
6. **Records async vs sync execution mode** per provider per document.

This framework is a TEST FACILITY — it MUST NOT be imported or invoked by production ingestion code.

#### FR3.7 — Long-Running Parse Timeout Configuration
Parser timeout configuration MUST support per-provider overrides:
```yaml
parser_services:
  marker_mcp:
    base_url: "https://marker0.cloud-dog.net"
    timeout_seconds: 300     # sync mode timeout
    async_threshold_seconds: 30  # switch to async above this
    async_poll_interval_seconds: 5
    async_max_wait_seconds: 600
  mineru:
    timeout_seconds: 300
    async_threshold_seconds: 60
    async_poll_interval_seconds: 10
    async_max_wait_seconds: 900
```

All timeout values MUST be configurable via `cloud_dog_config` / Vault (`dev.services.*`).

#### FR3.8 — Vault Config for Marker MCP (Confirmed Live)
The Vault entry `dev.services.marker_mcp` MUST contain:
- `base_url`: `https://marker0.cloud-dog.net`
- `auth_token`: (empty string or token if auth enabled)
- `timeout_seconds`: `300`
- `enabled`: `true`

This entry MUST be populated before Marker MCP tests can run.
