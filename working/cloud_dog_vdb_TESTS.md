# platform-vdb - PROPOSED Tests Uplift

**Package:** `cloud_dog_vdb`  
**Current Baseline:** 0.3.x  
**Proposed Target:** 0.4.0  
**Standards:** PS-60, PS-80, PS-90, PS-95  
**Status:** PROPOSED  
**Date:** 2026-02-28

---

## 1. Testing Objectives

This uplift adds parser/OCR/table/chunking coverage while proving full backward compatibility.

Primary gate: all existing 0.3.x suites continue to pass unchanged.

---

## 2. Test Hierarchy (Retained + Extended)

Retained suites:
- UT, ST, IT, AT, QT.

New suites:
- PT (Parser integration with external providers),
- CT (Compatibility/regression guarantees for 0.3.x behaviour).

All suite types follow PS-95 rules for env gating, sequencing, and reporting.

---

## 3. Proposed Suite Additions

### 3.1 UT2 - Parser/OCR/IR Unit Tests
- `UT2.1_ParserCapabilities` provider capability descriptor validation.
- `UT2.2_ParserPlannerSelection` deterministic chain selection.
- `UT2.3_ParserFallbackPolicy` fallback on error/timeout/quality miss.
- `UT2.4_DocumentIRSchema` IR structure and required provenance fields.
- `UT2.5_TableRenderPolicies` `text|markdown|html|json|dual` outputs.
- `UT2.6_TableJsonSchema` `rows_cols` and `records` schema validation.
- `UT2.7_OCRHeuristics` auto OCR decision thresholds.
- `UT2.8_OCRCostLimiter` cost caps and rate limiting.
- `UT2.9_BoundaryAwareChunking` do-not-split and boundary controls.
- `UT2.10_ConfigAliasCompatibility` old key aliases map to new config model.

### 3.2 ST2 - Local End-to-End Pipeline Behaviour
- `ST2.1_LegacyPathParity` legacy path output parity under default config.
- `ST2.2_IRPathLocal` parser-first path in local/in-memory mode.
- `ST2.3_CheckpointResumeAcrossParse` resumable pipeline at parse/chunk/embed boundaries.
- `ST2.4_TableChunkFlow` table policies drive chunk payload as configured.

### 3.3 PT1 - External Parser Integration
- `PT1.1_MineruAdapter` IR + tables + provenance.
- `PT1.2_DeepdocAdapter` enrichment fields and deterministic outputs.
- `PT1.3_DoclingAdapter` layout-aware extraction invariants.
- `PT1.4_MarkerMcpAdapter` MCP call contract and provenance capture.
- `PT1.5_OcrProviders` local/external/LLM OCR provider contracts.

PT rules:
- If provider is explicitly enabled and unreachable, test MUST fail.
- If provider is explicitly disabled, test MAY skip with explicit reason.

### 3.4 IT2 - Backend + Parser Pipeline Integration
For each enabled backend profile:
- ingest via parser-first path,
- verify metadata filters and retrieval,
- validate delete/purge lifecycle behaviour,
- verify fallback when backend capability is missing.

### 3.5 QT2 - Security for External Providers
- `QT2.1_CommandAllowlist` disallowed executable is blocked.
- `QT2.2_CommandTimeout` parser process timeout enforced.
- `QT2.3_EndpointAllowlist` outbound endpoint restrictions enforced.
- `QT2.4_SecretRedaction` no credential leakage in logs/audit.
- `QT2.5_PathTraversalGuard` unsafe local source paths rejected.

### 3.6 CT1 - Backward Compatibility Guarantees
- `CT1.1_PublicApiParity` existing APIs and signatures unchanged.
- `CT1.2_DefaultBehaviourParity` same input/config -> same chunk outputs as 0.3.x default path.
- `CT1.3_MetadataIdentityParity` deterministic IDs unchanged in legacy mode.
- `CT1.4_ConfigCompatibility` old config files still valid without edits.
- `CT1.5_ErrorContractParity` error codes/envelopes remain compatible.

---

## 4. Environment Profiles

Retained:
- `tests/env-UT`, `tests/env-ST`, `tests/env-IT`, `tests/env-AT`.

Proposed additions:
- `tests/env-PT` for parser integrations,
- `tests/env-CT` for compatibility baselines.

`--env` remains mandatory for all runs.

---

## 5. Execution Discipline

Required order for interactive verification:
1. UT1 + UT2
2. ST1 + ST2
3. CT1
4. IT1 + IT2
5. PT1
6. AT
7. QT1 + QT2

Stop on first failure and resolve before advancing.

---

## 6. Release Gates for 0.4.0

### Gate G1 - Baseline Stability
- Existing 0.3.x suites pass without modification.

### Gate G2 - New Feature Coverage
- All new UT2/ST2/IT2/PT1/QT2 suites pass in required environments.

### Gate G3 - Compatibility Proof
- CT1 suite passes fully.

### Gate G4 - Security Proof
- QT2 suite passes and redaction checks show no secret leakage.

### Gate G5 - Documentation Proof
- Updated docs include parser capability matrix, OCR policy guide, table policy guide, and migration notes.

---

## 7. Traceability Mapping (Proposed)

- `FR2.1-FR2.4` -> `UT2.1-UT2.3`, `PT1.1-PT1.4`
- `FR2.5` -> `UT2.4`, `ST2.2`
- `FR2.6-FR2.8` -> `UT2.7-UT2.8`, `PT1.5`, `QT2.4`
- `FR2.9-FR2.11` -> `UT2.5-UT2.6`, `UT2.9`, `ST2.4`
- `BC1-BC5` -> `CT1.1-CT1.5`, `ST2.1`
- `FR2.15` -> existing config-delegation checks + `QT2.4`

---

## 8. Test Deliverables for Implementation Phase

1. Add new suite directories and env files.
2. Add golden parser fixtures for IR/table/ocr invariants.
3. Add compatibility baseline fixtures from 0.3.x outputs.
4. Record run history in package `TESTS.md` and `CONTEXT-SUMMARY.md`.
