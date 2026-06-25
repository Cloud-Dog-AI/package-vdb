# W23A PT1.4 Re-run V2 Report

- Date (UTC): 2026-03-06T11:04:28Z
- Project: `platform-vdb`
- Scope: Re-run PT1.4 with timeout >= 900s
- Change policy: No source code, test files, or env files were modified.

## Command Executed

```bash
cd /opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb
set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a
timeout 900 python3 -m pytest tests/parser/PT1.4_MarkerMcpAdapter/ --env tests/env-PT -vv -rs 2>&1 \
  | tee working/w23a-pt1-4-rerun-v2.log
```

## Observed Result

- Test session started and collected 1 item:
  - `test_pt1_4_marker_mcp_corpus_quality_matrix`
- No further pytest output was emitted (no PASS/FAIL/XFAIL line, no ratio metrics, no final summary).
- Log content is only startup + test identifier (9 lines total).
- Marker health checked during/after run:
  - `{"status":"ok","busy":false,"inflight_requests":0,"workers":2,...}`

## Outcome vs Expected

Expected:
- No xfail
- `success_ratio >= 0.90`
- `quality_invariant_pass_rate >= 0.70`

Actual:
- Not verifiable from this run because the test did not produce completion output or final metrics.
- Based on runtime behavior and log ending at test start, this run appears to have not completed successfully within the allowed window (inferred from evidence).

## Evidence

- Run log:
  - `/opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb/working/w23a-pt1-4-rerun-v2.log`
- Report:
  - `/opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb/working/W23A-PT14-RERUN-V2-REPORT.md`
