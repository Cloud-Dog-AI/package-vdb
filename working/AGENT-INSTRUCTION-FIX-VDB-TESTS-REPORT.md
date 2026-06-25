# AGENT-INSTRUCTION-FIX-VDB-TESTS — Completion Report

Date (UTC): 2026-03-11
Package: `packages/backend/platform-vdb`
Instruction: `AGENT-INSTRUCTION-FIX-VDB-TESTS.md`

## Summary

The remaining blocker was ST3 appearing to "hang" with no pytest output. Root cause was long-running live Marker OCR calls (roughly 3.8–4.0 minutes for ST3.1/ST3.2), not a deadlock. A secondary correctness defect was found in ST3 corpus entry selection: tests silently fell back to the first corpus item when the required ID was absent from the active slice.

Fix applied:
- Made ST3 entry lookup strict (fail-fast when required corpus ID is missing) in:
  - `tests/system/ST3.1_MarkerMcpSyncParse/test_st3_1_marker_mcp_sync_parse.py`
  - `tests/system/ST3.2_MarkerMcpAsyncParse/test_st3_2_marker_mcp_async_parse.py`
  - `tests/system/ST3.3_MarkerMcpImageArtefacts/test_st3_3_marker_mcp_image_artefacts.py`
  - `tests/system/ST3.4_MarkerMcpLargeDocument/test_st3_4_marker_mcp_large_document.py`
- Updated `TESTS.md`:
  - ST3.4 description now uses manifest-driven "large/complex corpus document" wording.
  - Added 2026-03-11 run history rows with explicit pass/fail/skip counts and command evidence.

## Completion Gate Verification

1. `env-IT` no longer causes silent skips; missing Vault vars fail in IT/AT
- Status: PASS
- Evidence: `tests/conftest.py` enforces hard fail for IT/AT preconditions (`pytest.fail`, not silent skip).

2. All 10 misclassified tests moved to correct directories
- Status: PASS
- Evidence: current test tree and prior reclassification records in `TESTS.md` show UT/ST targets (`UT1.38`–`UT1.43`, `ST1.9`–`ST1.12`) and replacement IT/AT/QT suites active.

3. Real-backend replacements exist for all reclassified IT/AT/QT tests
- Status: PASS
- Evidence: rerun logs show active replacement suites passing in real tiers:
  - `working/fix-vdb-tests-it-at-rerun.log` (31 passed)
  - `working/fix-vdb-tests-qt-rerun.log` (12 passed)

4. Write-path probe fixtures exist for all five backends
- Status: PASS
- Evidence: `tests/conftest.py` includes backend readiness fixtures used by IT/QT (`chroma_ready`, `qdrant_ready`, `weaviate_ready`, `opensearch_ready`, `pgvector_ready`).

5. `TESTS.md` updated with correct classifications and honest run history
- Status: PASS
- Evidence: 2026-03-11 entries added under Run History; ST3.4 definition corrected; pass/fail/skip explicitly recorded.

6. Full UT/ST suite runs with `0 skipped`
- Status: PASS
- Command:
  - `set -o pipefail; timeout 10800 .venv/bin/pytest tests/unit tests/system --env tests/env-UT --env tests/env-ST -v --durations=0 | tee working/fix-vdb-tests-ut-st-rerun.log`
- Result:
  - `102 passed, 0 failed, 0 skipped`
  - `__EXIT_CODE__=0`

7. IT/AT runs against real backends with honest reporting
- Status: PASS
- Command:
  - `set -o pipefail; set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 5400 .venv/bin/pytest tests/integration tests/application --env tests/env-IT --env tests/env-AT -v --durations=0 | tee working/fix-vdb-tests-it-at-rerun.log`
- Result:
  - `31 passed, 0 failed, 0 skipped`
  - `__EXIT_CODE__=0`

Additional security gate rerun:
- Command:
  - `set -o pipefail; set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 1800 .venv/bin/pytest tests/security --env tests/env-IT -v --durations=0 | tee working/fix-vdb-tests-qt-rerun.log`
- Result:
  - `12 passed, 0 failed, 0 skipped`
  - `__EXIT_CODE__=0`

## ST3 Timing Evidence (hang diagnosis)

From `working/fix-vdb-tests-st3-rerun-envst.log`:
- ST3.1: `238.88s`
- ST3.2: `232.17s`
- ST3.3: `19.18s`
- ST3.4: `18.36s`
- Total: `4 passed in 509.02s`, `__EXIT_CODE__=0`

Interpretation: no deadlock; test runtime reflects live OCR latency on CPU-backed service.
