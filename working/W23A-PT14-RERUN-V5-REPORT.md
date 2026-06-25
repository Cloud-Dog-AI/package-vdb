# W23A PT1.4 Rerun V5 Report

## Scope
Diagnose V4 no-output hang and produce a real terminal result for PT1.4 Marker MCP using project venv interpreter.

## Diagnostic findings (V4 hang cause)
1. V4 used the wrong interpreter (`/usr/bin/python3`) instead of project venv (`.venv/bin/python`).
2. V4 log confirms collection reached 1 test then stalled with no terminal outcome line:
   - `working/W23A-PT14-RERUN-V4.log` shows test node printed with no PASS/FAIL/XFAIL summary.
3. The V5 diagnostic command from instruction included `--timeout=90`; in the venv this fails immediately because `pytest-timeout` is not installed:
   - `error: unrecognized arguments: --timeout=90`
4. With the correct venv interpreter and no unsupported arg, the rerun completed quickly and deterministically as `XFAIL` due to provider-busy gating.

## Result (real terminal outcome)
- Test: `tests/parser/PT1.4_MarkerMcpAdapter/test_pt1_4_marker_mcp_adapter.py::test_pt1_4_marker_mcp_corpus_quality_matrix`
- Outcome: `XFAIL`
- Evidence: `working/W23A-PT14-RERUN-V5.log`
- Summary line: `1 xfailed in 0.39s`
- XFAIL reason (from `-rxX` rerun): `marker_mcp busy: inflight=1, workers=1, status=ok`

## marker0 health before/after
- Before rerun:
```json
{
  "status": "ok",
  "busy": true,
  "inflight_requests": 1,
  "workers": 1,
  "ocr_queue_wait_seconds": 30.0,
  "ocr_timeout_seconds": 900.0
}
```
- After rerun (2026-03-07T09:14:37Z):
```json
{
  "status": "ok",
  "busy": true,
  "inflight_requests": 1,
  "workers": 1,
  "ocr_queue_wait_seconds": 30.0,
  "ocr_timeout_seconds": 900.0
}
```

## success_ratio and quality_invariant_pass_rate
- `success_ratio`: not emitted in V5 rerun log.
- `quality_invariant_pass_rate`: not emitted in V5 rerun log.
- Reason: test exited at provider-busy `XFAIL` gate before corpus quality matrix metrics were computed/printed.

## Files
- `working/W23A-PT14-V5-diagnostic.log`
- `working/W23A-PT14-RERUN-V5.log`
- `working/W23A-PT14-RERUN-V4.log`
