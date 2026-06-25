# platform-vdb 0.4.0 - Development, Build, and Test Programme

**Package:** `cloud_dog_vdb`  
**Status:** Executed and verified (W13A closeout)  
**Date:** 2026-03-01  
**Applies to:** parser chain uplift, OCR uplift, table policies, Infinity backend, full backward compatibility

---

## 1. Non-Negotiable Objectives

1. Deliver parser and OCR extensibility (MinerU, marker-mcp, DeepDoc, Docling, internal parser) without breaking existing users.
2. Add Infinity backend support behind the existing adapter contract.
3. Validate quality and performance on committed real-world corpus in `test-data/`.
4. Achieve 100% backward compatibility proof through dedicated compatibility suites.
5. Keep PS-60, PS-80, PS-90, and PS-95 compliance.

---

## 2. Delivery Scope

### In scope
- Parser provider interfaces, registries, and deterministic fallback.
- OCR providers and auto/force/disabled policy.
- Table extraction/render policies and chunking controls.
- Infinity adapter + capability descriptor + conformance tests.
- Corpus-driven parser quality/performance tests.
- Build/release workflow and CI gates.

### Out of scope
- Breaking domain API changes.
- Mandatory installation of all parser providers.
- Changes to deterministic identity formula used by 0.3.x mode.

---

## 3. Workstream Plan

## WS1 - Core implementation
- Add `ingestion/parse/*`, `ingestion/ocr/*`, `ingestion/table/*`, `ingestion/chunk/boundary.py`.
- Add compatibility bridge `compat/ingestion_v1.py`.
- Add Infinity adapter `adapters/infinity.py` and register in adapter factory.

## WS2 - Config and runtime integration
- Extend config models for parser chain, OCR policy, table policy, chunk policy.
- Ensure config delegation only through `cloud_dog_config`.
- Integrate endpoint sourcing from env files and Vault (`dev.services.*`, `dev.vdbs.infinity`).

## WS3 - Test implementation
- Add UT2/ST2/IT2/PT1/PT2/QT2/CT1 suites.
- Add corpus-manifest driven invariants and benchmark harness.
- Add Infinity backend test profile to integration and compatibility suites.

## WS4 - Build and release
- Run lint/format/tests sequentially.
- Produce wheel and sdist.
- Install built wheel and run smoke verification.
- Finalise release notes and migration notes.

## WS5 - Documentation and agent guidance
- Maintain operator/agent runbook (`AGENTS.md`) for compliant execution using real systems.
- Update `README.md`, `TESTS.md`, and `RELEASE_UPLIFT_PROPOSAL.md` whenever execution order, gates, or blockers change.
- Record run evidence (commands, dates, results, blockers) in `TESTS.md` run history before release sign-off.

---

## 4. Test Catalogue to Implement

## 4.1 Unit (UT2)
- `UT2.1_ParserCapabilities`
- `UT2.2_ParserPlannerSelection`
- `UT2.3_ParserFallbackPolicy`
- `UT2.4_DocumentIRSchema`
- `UT2.5_TableRenderPolicies`
- `UT2.6_TableJsonSchema`
- `UT2.7_OCRHeuristics`
- `UT2.8_OCRCostLimiter`
- `UT2.9_BoundaryAwareChunking`
- `UT2.10_ConfigAliasCompatibility`
- `UT2.11_ParserCommandSandboxPolicy`
- `UT2.12_ProviderHealthContract`
- `UT2.13_BatchUpsertLifecycleParity`
- `UT2.14_InfinityOutputShapeCompatibility`
- `UT2.15_SourceUriFilenameMimeInference`

## 4.2 System (ST2)
- `ST2.1_LegacyPathParity`
- `ST2.2_IRPathLocal`
- `ST2.3_CheckpointResumeAcrossParse`
- `ST2.4_TableChunkFlow`
- `ST2.5_OcrModeDisabled`
- `ST2.6_OcrModeForce`
- `ST2.7_OcrModeAuto`
- `ST2.8_ParserFallbackExecution`

## 4.3 Integration (IT2)
- `IT2.1_ParserFirstIngest_Chroma`
- `IT2.2_ParserFirstIngest_Qdrant`
- `IT2.3_ParserFirstIngest_Weaviate`
- `IT2.4_ParserFirstIngest_OpenSearch`
- `IT2.5_ParserFirstIngest_PGVector`
- `IT2.6_ParserFirstIngest_Infinity`
- `IT2.7_DeleteByFilterPortableFallback`
- `IT2.8_LifecycleAndPurgeSafety`
- `IT2.9_MetadataFilterParity`
- `IT2.10_EmbeddingDimensionValidation`

## 4.4 Parser integration (PT1)
- `PT1.1_MineruAdapter`
- `PT1.2_DeepdocAdapter`
- `PT1.3_DoclingAdapter`
- `PT1.4_MarkerMcpAdapter`
- `PT1.5_OcrProviders`
- `PT1.6_EndpointSource_EnvFile`
- `PT1.7_EndpointSource_VaultResolved`

## 4.5 Performance/quality (PT2)
- `PT2.1_CorpusLatencyBenchmarks`
- `PT2.2_CorpusThroughputBenchmarks`
- `PT2.3_OcrAutoDecisionRate`
- `PT2.4_QualityInvariantPassRate`
- `PT2.5_ParserComparisonMatrix`
- `PT2.6_EmbeddingPipelinePerformance`

## 4.6 Security (QT2)
- `QT2.1_CommandAllowlist`
- `QT2.2_CommandTimeout`
- `QT2.3_EndpointAllowlist`
- `QT2.4_SecretRedaction`
- `QT2.5_PathTraversalGuard`
- `QT2.6_OcrCostGuard`
- `QT2.7_ConfigDelegationEnforcement`

## 4.7 Compatibility (CT1)
- `CT1.1_PublicApiParity`
- `CT1.2_DefaultBehaviourParity`
- `CT1.3_MetadataIdentityParity`
- `CT1.4_ConfigCompatibility`
- `CT1.5_ErrorContractParity`
- `CT1.6_ResultEnvelopeParity`
- `CT1.7_LegacyConverterParity`

---

## 5. Backward Compatibility Proof Strategy

A 0.4.0 build is not eligible for release unless all are true:
1. Existing UT1/ST1/IT1/AT1/QT1 suites pass unchanged.
2. CT1 suite passes entirely.
3. Default config path executes legacy behaviour parity checks.
4. New functionality only activates when explicitly configured.

---

## 6. Environment and Config Matrix

Required non-secret env files:
- `tests/env-UT`
- `tests/env-ST`
- `tests/env-IT`
- `tests/env-AT`
- `tests/env-PT`
- `tests/env-PT-PERF`
- `tests/env-CT`

Required Vault sections:
- `dev.vdbs.chroma`
- `dev.vdbs.qdrant`
- `dev.vdbs.weaviate`
- `dev.vdbs.opensearch`
- `dev.vdbs.pgvector`
- `dev.vdbs.infinity`
- `dev.services.mineru`
- `dev.services.marker_mcp` or `dev.services.markermcp`
- embedding model sections used by parser-first ingestion tests

---

## 7. Build and Test Execution Programme

Run order (strict):
1. `ruff check`
2. `ruff format --check`
3. `pytest tests/unit tests/system --env tests/env-UT --env tests/env-ST`
4. `pytest tests/compatibility --env tests/env-CT`
5. `pytest tests/integration --env tests/env-IT`
6. `pytest tests/parser --env tests/env-PT`
7. `pytest tests/parser_performance --env tests/env-PT-PERF`
8. `pytest tests/application --env tests/env-AT`
9. `pytest tests/security --env tests/env-IT`
10. `python -m build`
11. `pip install dist/*.whl`
12. package import and smoke checks
13. documentation stage: update `README.md`, `TESTS.md`, `AGENTS.md`, release/migration notes with current evidence

All integration-like tiers must run with sourced Vault env when required.

### W13A Strict Closeout Evidence (2026-03-01)

- Strict provider enforcement gates passed:
  - `IT2.11` with `REQUIRE_ALL_PDF_PARSERS=true`
  - `AT2.2` with `REQUIRE_ALL_PDF_PARSERS=true`
- Large-corpus parser and parser-performance gates passed under timeout guards:
  - `timeout 1800 pytest tests/parser --env tests/env-PT --env tests/env-CORPUS-LARGE -q`
  - `timeout 1800 pytest tests/parser_performance --env tests/env-PT-PERF --env tests/env-CORPUS-LARGE -q`
- Build/install/smoke gates passed for `cloud_dog_vdb==0.4.0`.
- Exact command logs are captured in `/tmp/w13a_*.log` and summarized in:
  - `TESTS.md` run history
  - `working/W13A-PLATFORM-VDB-0.4.0-RELEASE-CLOSEOUT-REPORT-2026-03-01.md`

### W25A-75 Marker MCP Transport Validation (2026-03-08)

- Marker pre-flight checks passed (`/health` and MCP `initialize` on `/mcp`).
- Lint gates passed after formatting correction in `UT3.8` test file.
- UT/CT/PT/AT/QT gates passed.
- ST and IT gates required live-service stabilisation reruns for long-running marker/integration cases:
  - ST3.1 initially failed with `OCR worker busy; queue wait exceeded 30s`, then passed on rerun.
  - ST3.2 completed in ~3m49s on controlled rerun.
  - IT full run stalled at `IT2.7`; tail tests (`IT2.7/2.8/2.9`) passed under isolated timeout-controlled reruns.
- Build/install/smoke passed for `cloud_dog_vdb==0.5.0`.
- Command evidence and matrix reporting captured in:
  - `working/W25A-75-MARKER-MCP-VALIDATE-REPORT.md`

---

## 8. CI Gate Design

Mandatory gates:
- Gate A: lint and format.
- Gate B: UT/ST.
- Gate C: CT1 compatibility.
- Gate D: IT matrix including Infinity.
- Gate E: PT1 parser integration.
- Gate F: QT1+QT2 security.
- Gate G: build artefact validation.
- Gate I: documentation and evidence consistency (`README.md`/`TESTS.md`/`AGENTS.md` aligned with executed runs).

Optional scheduled gate:
- Gate H: PT2 performance baseline trend runs.

---

## 9. Deliverables Checklist

- [x] Parser/OCR/table/Infinity implementation merged.
- [x] Test suites implemented and passing.
- [x] Corpus manifest and benchmark harness implemented.
- [x] Backward compatibility proof report complete.
- [x] Build and install verification complete.
- [x] TESTS.md and run history updated.
- [x] Release notes and migration notes updated.
- [x] Agent guidance (`AGENTS.md`) updated and aligned with release execution evidence.
