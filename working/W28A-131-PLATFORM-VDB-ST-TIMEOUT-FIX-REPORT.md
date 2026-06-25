# W28A-131 — platform-vdb ST Timeout Fix Report

Date (UTC): 2026-03-11  
Project: `packages/backend/platform-vdb`

## 1) Root cause

The ST timeout was a runtime-threshold problem, not a functional ST failure.

Evidence from timed run:
- `tests/system` completed successfully with `24 passed in 536.44s` using:
  - `--env tests/env-ST`
  - `--durations=0`
- Slowest tests:
  - `ST3.1_MarkerMcpSyncParse`: `237.25s`
  - `ST3.2_MarkerMcpAsyncParse`: `228.19s`
- Combined, these two tests consume ~465s before remaining ST workload.

Given observed variance in prior runs, a 600s cap is brittle and can exceed in normal remote-service conditions.

Notes:
- Prior #122 ST log is partial (`21 bytes`, dots), consistent with process kill during execution.
- If `--env` were missing, pytest would fail fast with usage/config error rather than run for 600s and timeout.

## 2) Fix applied

No source/test logic changes were needed for ST behaviour.

Operational/test-run fix:
- Use a higher ST timeout for sweep execution (`>=1200s`).
- Ensure sweep command includes mandatory `--env tests/env-ST`.

Instruction command correction applied during verification:
- `tests/quality/` does not exist in this package.
- QT was run via `tests/security/` (actual package location).

## 3) Test durations (`--durations=0`)

From `working/w28a-131-st-timed.log`:

- 237.25s — `ST3.1_MarkerMcpSyncParse::test_st3_1_marker_mcp_sync_parse_live`
- 228.19s — `ST3.2_MarkerMcpAsyncParse::test_st3_2_marker_mcp_async_mode_fallback_live`
- 18.32s — `ST2.2_IRPathLocal::test_st2_2_ir_path_local_mineru`
- 17.97s — `ST2.1_LegacyPathParity::test_st2_1_mineru_and_internal_parser_path`
- 16.80s — `ST3.4_MarkerMcpLargeDocument::test_st3_4_marker_mcp_large_document_live`
- 16.03s — `ST3.3_MarkerMcpImageArtefacts::test_st3_3_marker_mcp_extracts_image_artefacts_live`

Timed ST total:
- `24 passed in 536.44s (0:08:56)`

## 4) Test results (REAL counts)

- QT:
  - `tests/quality/` => path missing (instruction path mismatch)
  - `tests/security/` => `12 passed`
- UT: `81 passed`
- ST (timed): `24 passed`
- ST (final): `24 passed`
- IT: `24 passed`
- AT: `7 passed`

## 5) Recommended minimum timeout for ST

Recommended ST timeout: **1200 seconds** (20 minutes).

Rationale:
- Current measured baseline ~536–550s with service-dependent variance.
- 600s leaves insufficient headroom for marker/MinerU latency spikes.
- 1200s provides safe margin while still bounded.

## 6) Verdict

**PASS**

platform-vdb ST timeout issue is resolved as a sweep/runtime configuration correction (timeout threshold and env-path correctness), with all tiers passing.

## 7) Evidence logs

- `working/w28a-131-st-timed.log`
- `working/w28a-131-qt.log`
- `working/w28a-131-ut.log`
- `working/w28a-131-st-final.log`
- `working/w28a-131-it.log`
- `working/w28a-131-at.log`

I warrant that:
1. I have read RULES.md IN FULL before starting work
2. ALL code I produced is 100% compliant with RULES.md
3. ALL test results reported are REAL — exact counts from actual runs
4. I have NOT weakened any test
5. I have NOT stored, copied, or exposed any credentials
6. ALL credentials come from Vault or git-ignored env files
7. I have NOT modified files outside this package
