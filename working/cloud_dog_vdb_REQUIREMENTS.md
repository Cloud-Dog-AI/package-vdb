# platform-vdb - PROPOSED Requirements Uplift

**Package:** `cloud_dog_vdb`  
**Current Baseline:** 0.3.x  
**Proposed Target:** 0.4.0  
**Standards:** PS-60, PS-75, PS-80, PS-90, PS-95  
**Status:** PROPOSED  
**Date:** 2026-02-28

---

## 1. Scope

This document proposes additive requirements for extending ingestion and indexing to support multiple internal and external parser backends, OCR strategies (including external LLM OCR), richer chunk/table controls, and stronger provenance.

All existing 0.3.x requirements remain in force unless explicitly superseded. This uplift is additive and must preserve 100% backward compatibility.

---

## 2. Compatibility Contract (Non-Negotiable)

### BC1 - API Compatibility
- Public Python APIs used by current consumers MUST remain stable in 0.4.0.
- Existing method signatures MUST continue to work without caller changes.
- New functionality MUST be opt-in via new config/options.

### BC2 - Behavioural Compatibility by Default
- If no new parser/OCR options are set, ingestion behaviour MUST match 0.3.x defaults.
- Existing converter+chunker paths MUST still execute successfully.

### BC3 - Configuration Compatibility
- Existing config keys MUST remain accepted.
- New config keys MUST be additive.
- If key renames are introduced, alias mapping MUST be provided with deprecation warnings.

### BC4 - Metadata/Identity Compatibility
- Existing canonical metadata fields and deterministic ID rules MUST remain unchanged.
- New provenance fields MUST be additive and optional.

### BC5 - Failure Compatibility
- Existing error taxonomy and portable error envelope MUST remain stable.
- New parser/OCR specific errors MUST be mapped into existing top-level taxonomy categories.

---

## 3. Proposed Functional Uplift

### FR2.1 - Parser Provider Framework
The package MUST support parser providers via a common interface with four execution modes:
- in-process Python provider,
- external command provider,
- external HTTP provider,
- external MCP provider.

Each provider MUST expose:
- `provider_id`,
- version,
- capability descriptor,
- health check,
- extraction entrypoint returning Document IR.

### FR2.2 - Supported Provider Set
The package MUST provide first-class adapters or integration contracts for:
- MinerU,
- marker-mcp,
- DeepDoc,
- Docling,
- baseline internal parser.

Pandoc integration SHOULD be supported where available.

### FR2.3 - Parser Chain and Fallback Policies
A configurable parser chain MUST be supported per profile/collection.

Fallback triggers MUST support:
- provider error,
- timeout,
- quality threshold miss,
- missing required capability (for example table extraction requested but unavailable).

Fallback policy MUST be deterministic and auditable.

### FR2.4 - Parser Capability Model
Provider capabilities MUST include at least:
- `supports_pdf`, `supports_docx`, `supports_html`,
- `supports_layout`, `supports_tables`, `supports_images`,
- `supports_ocr_passthrough`,
- `supports_streaming`,
- `max_document_bytes`.

### FR2.5 - Document IR (Intermediate Representation)
The pipeline MUST standardise on a Document IR between parsing and chunking.

IR MUST support:
- text blocks,
- heading hierarchy,
- list blocks,
- table blocks,
- optional image/figure references,
- provenance fields,
- parser and OCR quality metrics.

### FR2.6 - OCR Framework
OCR MUST be a pluggable optional stage with modes:
- `disabled`,
- `auto`,
- `force`.

OCR provider types MUST include:
- local engine,
- external service,
- external LLM OCR.

### FR2.7 - OCR Decision Heuristics
When OCR mode is `auto`, heuristics MUST support configurable thresholds such as:
- minimum extracted text density,
- scanned-page ratio,
- parser confidence.

Decision outcomes MUST be recorded in metadata/audit.

### FR2.8 - OCR Cost and Safety Controls
LLM OCR integration MUST support:
- per-document and per-job cost caps,
- rate limiting,
- timeout and retry policy,
- explicit disable flag by profile/environment.

### FR2.9 - Table Handling Policies
Table handling MUST support:
- `table_as_text`,
- `table_as_markdown`,
- `table_as_html`,
- `table_as_json`,
- `table_dual`.

For `table_as_json`, supported output shapes MUST include:
- `rows_cols`,
- `records`.

### FR2.10 - Table Chunking Controls
Chunking MUST support table-aware policies:
- `whole_table`,
- `row_chunks` with configurable maximum rows per chunk.

### FR2.11 - Chunking Controls Uplift
Chunking MUST support explicit options for:
- chunk unit: `tokens|characters|bytes`,
- chunk size,
- overlap,
- separator priorities,
- boundary source preference (`heading`, `paragraph`, `page`, `table_row`, `semantic`),
- do-not-split guards (tables, code blocks, heading+lead paragraph).

### FR2.12 - Provenance Requirements
Each emitted chunk MUST include additive provenance fields:
- parser provider id/version,
- OCR provider id/version (when used),
- page range where available,
- section path where available,
- table locator where applicable,
- chunk kind (`text`, `table`, `ocr_text`, `caption`, etc.).

### FR2.13 - Capability-Aware Planning
Ingestion planning MUST select parser/OCR/table/chunk strategy based on:
- requested policy,
- provider capabilities,
- profile constraints,
- file type and size.

### FR2.14 - Extensibility Contract
New providers MUST be registerable without changing core domain API or existing adapter interfaces.

### FR2.15 - Config Delegation and Secret Handling
Parser and OCR credentials/endpoints MUST be pre-resolved by `cloud_dog_config`.

`cloud_dog_vdb` MUST NOT:
- read Vault directly,
- import `hvac`,
- parse Vault JSON,
- read credential environment variables directly.

### FR2.16 - Observability Extensions
New ingestion events MUST emit:
- provider selection,
- fallback reason,
- OCR decisions,
- table policy used,
- chunking policy used,
- timings per stage,
- cost counters where relevant.

### FR2.17 - Documentation Uplift Requirement
Release 0.4.0 documentation MUST include:
- parser capability matrix,
- OCR mode/provider guide,
- table handling guide,
- backward-compatibility migration notes,
- example configurations for all supported providers.

---

## 4. Non-Functional Requirements

### NFR2.1
New parser/OCR integrations MUST not introduce mandatory dependencies for default installs.

### NFR2.2
Provider integrations MUST fail fast with explicit actionable errors when enabled but unavailable.

### NFR2.3
The parser chain MUST be deterministic for identical inputs and configuration.

### NFR2.4
Security controls for external command parsers MUST include allowlists, timeout, and bounded temp workspace usage.

---

## 5. Proposed Configuration Additions (Additive)

Recommended additive keys under `vector_stores_config.ingestion`:
- `parser_chain` (ordered list),
- `parser_fallback_policy`,
- `ocr.mode`,
- `ocr.provider`,
- `ocr.heuristics.*`,
- `ocr.cost_limits.*`,
- `tables.policy`,
- `tables.json_shape`,
- `tables.chunk_mode`,
- `chunking.unit`,
- `chunking.size`,
- `chunking.overlap`,
- `chunking.separators`,
- `chunking.do_not_split.*`.

All keys are optional; defaults preserve 0.3.x behaviour.

---

## 6. Acceptance Criteria

1. Existing 0.3.x tests pass unchanged.
2. New parser/OCR/table/chunk functionality is fully covered by new suites.
3. Backward-compatibility suites prove API, config, and behavioural stability.
4. Package builds and docs are updated for 0.4.0 release.

---

## 7. Out of Scope for This Uplift

- Breaking API redesign of existing VDB operations.
- Mandatory installation of all parser providers.
- Replacement of existing deterministic identity rules.
