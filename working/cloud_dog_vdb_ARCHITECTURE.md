# platform-vdb - PROPOSED Architecture Uplift

**Package:** `cloud_dog_vdb`  
**Current Baseline:** 0.3.x  
**Proposed Target:** 0.4.0  
**Standards:** PS-10, PS-60, PS-80, PS-90, PS-95  
**Status:** PROPOSED  
**Date:** 2026-02-28

---

## OV1 - Uplift Summary

The 0.4.0 uplift adds a parser-first ingestion architecture with OCR, table-aware processing, and richer chunking controls, while preserving all existing 0.3.x interfaces and default behaviour.

Key architectural principle: new functionality is additive and policy-driven; existing consumers continue to work unchanged.

---

## SA1 - Additive Module Layout

```text
cloud_dog_vdb/
  ingestion/
    pipeline.py                    # existing orchestrator, extended only
    convert/                       # existing path retained for compatibility
      base.py
      pandas_conv.py
      deepdoc_conv.py
      mineru_conv.py

    parse/                         # new parser framework
      base.py                      # ParserProvider protocol
      capabilities.py              # ParserCapability model
      registry.py                  # provider registry + chain resolver
      planner.py                   # deterministic parser selection
      quality.py                   # quality scoring and thresholds
      ir.py                        # DocumentIR, IRBlock, TableBlock, Provenance
      normalise.py                 # IR canonicalisation helpers
      providers/
        internal.py                # baseline internal parser
        mineru.py                  # MinerU adapter
        deepdoc.py                 # DeepDoc adapter
        docling.py                 # Docling adapter
        marker_mcp.py              # marker-mcp adapter
        pandoc.py                  # optional adapter

    ocr/                           # new OCR subsystem
      base.py                      # OCRProvider protocol
      registry.py                  # provider registration
      planner.py                   # OCR mode decision (disabled/auto/force)
      heuristics.py                # scanned-page/text-density heuristics
      providers/
        local.py
        external_service.py
        llm.py

    table/
      policy.py                    # table output policy + schema mode
      renderers.py                 # text/md/html/json/dual renderers
      schema.py                    # JSON schema validation

    chunk/
      base.py                      # existing interface
      fixed.py                     # existing
      recursive.py                 # existing
      semantic.py                  # existing
      boundary.py                  # new boundary-aware helpers

  compat/
    ingestion_v1.py                # legacy convert->chunk bridge
```

Notes:
- Existing modules remain valid and import paths stay stable.
- New modules are optional execution paths used only when configured.

---

## SA2 - Runtime Flow (Proposed)

### SA2.1 Default Compatibility Path (unchanged)
`acquire -> convert -> chunk -> embed -> upsert`

Used when no parser-chain options are configured.

### SA2.2 Extended Parser-First Path
`acquire -> parser_chain -> OCR(optional) -> DocumentIR -> table policy -> chunk -> embed -> upsert -> verify -> lifecycle`

Pipeline stages remain checkpointable and observable.

### SA2.3 Deterministic Provider Selection
Selection inputs:
- file type/source type,
- required capabilities,
- configured provider order,
- profile constraints,
- health/availability checks.

Output:
- chosen provider,
- fallback provider list,
- deterministic execution plan id (for traceability).

---

## CC1 - Core Components

### CC1.1 ParserProvider
Responsibilities:
- parse source into Document IR,
- declare capabilities,
- expose provider metadata/version,
- emit quality metrics.

### CC1.2 ParserRegistry and ParserPlanner
Responsibilities:
- resolve provider chain,
- enforce fallback policy,
- prevent non-deterministic provider selection,
- record selection decisions for audit.

### CC1.3 OCRPlanner and OCRProvider
Responsibilities:
- apply `disabled/auto/force` policy,
- evaluate heuristics in `auto` mode,
- call selected OCR provider,
- attach OCR provenance/cost/timing.

### CC1.4 DocumentIR
Responsibilities:
- represent parsed artefacts in a backend-agnostic intermediate model,
- carry provenance and quality metadata,
- provide stable inputs into chunking and identity logic.

### CC1.5 TablePolicyEngine
Responsibilities:
- enforce `table_as_*` policy,
- validate JSON table schema mode,
- generate table chunks with provenance.

### CC1.6 Backward Compatibility Bridge
`compat/ingestion_v1.py` responsibilities:
- adapt legacy converter outputs into minimal IR when needed,
- preserve historical chunk semantics unless explicitly overridden,
- ensure old config keys still map to valid execution settings.

---

## DM1 - Data Model Additions (Additive)

### DM1.1 DocumentIR (new)
Core fields:
- `document_id`, `source_uri`, `media_type`,
- `blocks[]` (typed blocks),
- `tables[]` (structured table objects),
- `provenance` (provider, pages, sections),
- `quality` (scores/confidence),
- `artefact_refs[]` (optional).

### DM1.2 Chunk Provenance Extension (new optional metadata)
- `parser_provider`, `parser_version`,
- `ocr_provider`, `ocr_version`,
- `page_start`, `page_end`,
- `section_path`,
- `table_ref`,
- `chunk_kind`.

Identity-critical fields from 0.3.x remain unchanged.

---

## DF1 - Configuration and Dependency Boundaries

### DF1.1 Config Delegation
All endpoints, credentials, and secrets for parser/OCR providers are resolved by `cloud_dog_config` before entering `cloud_dog_vdb`.

`cloud_dog_vdb` must not perform direct Vault resolution.

### DF1.2 Optional Dependencies
All external parser/OCR provider libraries remain optional extras.
Missing extras must produce clear runtime errors only when the provider is explicitly enabled.

---

## SE1 - Security Controls for External Parsers

Required controls:
- command allowlist for command-based providers,
- timeout and maximum input-size bounds,
- temp workspace isolation,
- endpoint allowlist for HTTP/MCP providers,
- sensitive field redaction in logs,
- audit entries for provider execution and fallback.

---

## RR1 - Failure and Recovery

### RR1.1 Failure Modes
- provider unavailable,
- provider timeout,
- low quality output,
- OCR provider quota/cost limit reached,
- invalid table schema emission.

### RR1.2 Recovery
- deterministic fallback to next provider in chain,
- configurable fail-fast for mandatory capabilities,
- checkpointed resume from parse/chunk/embed boundaries.

---

## MU1 - Migration and Rollout Strategy

### MU1.1 Phase 1 (Compatibility Release)
- ship parser framework behind disabled-by-default config,
- keep legacy path as default.

### MU1.2 Phase 2 (Controlled Adoption)
- enable parser-chain per selected collections,
- collect quality/performance/cost telemetry.

### MU1.3 Phase 3 (Wider Adoption)
- progressively enable parser-chain defaults where validated,
- retain explicit opt-out to legacy path until deprecation milestone is approved.

---

## Acceptance Gate (Architecture)

1. Legacy ingestion path remains functionally unchanged under default config.
2. Parser-first path is fully policy-driven and deterministic.
3. External parser integrations are sandboxed and auditable.
4. Provenance extensions are additive and do not break existing metadata consumers.
