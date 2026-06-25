# W23A PT1.4 Re-run V4 Report

- Date (UTC): 2026-03-06T14:32:48Z
- Project: `platform-vdb`
- Scope: PT1.4 rerun with `env-CORPUS-SMALL` (5 small docs)
- Change policy: No source code, test files, or env files modified

## Phase 1: marker0 health before run

Command:

```bash
curl -sf https://marker0.cloud-dog.net/health
```

Observed:
- First probes were intermittently unreachable (DNS/route from this shell).
- Successful pre-run health response:

```json
{"status":"ok","busy":false,"inflight_requests":0,"workers":1,"ocr_queue_wait_seconds":30.0,"ocr_timeout_seconds":900.0,"uptime_seconds":7163.1}
```

## Phase 2: PT1.4 with SMALL corpus

Command:

```bash
cd /opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb
set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a
timeout 900 python3 -m pytest tests/parser/PT1.4_MarkerMcpAdapter/ \
  --env tests/env-PT --env tests/env-CORPUS-SMALL -x -v -s 2>&1 | tee working/W23A-PT14-RERUN-V4.log
```

Observed test output:
- Pytest started and collected 1 test:
  - `test_pt1_4_marker_mcp_corpus_quality_matrix`
- No further output emitted (no PASS/FAIL/XFAIL line, no final pytest summary).
- Log file has 9 lines total (session header + test identifier only).

## Phase 3: soft-fail check + post-run health

Soft-fail grep command:

```bash
grep -E "xfail|XFAIL|busy|unhealthy|soft_fail" working/W23A-PT14-RERUN-V4.log
```

Observed:
- No matches.

Post-run health command:

```bash
curl -sf https://marker0.cloud-dog.net/health
```

Observed:
- Intermittent unreachable responses from this shell, then successful response:

```json
{"status":"ok","busy":false,"inflight_requests":0,"workers":1,"ocr_queue_wait_seconds":30.0,"ocr_timeout_seconds":900.0,"uptime_seconds":8236.9}
```

## Required Outputs

- Pass/fail/xfail result: **FAIL (inconclusive/timeout-like run; no pytest verdict emitted)**
- marker0 health before: **status=ok, busy=false, workers=1**
- marker0 health after: **status=ok, busy=false, workers=1**
- `success_ratio`: **not present in output**
- `quality_invariant_pass_rate`: **not present in output**

## Artifacts

- Log: `/opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb/working/W23A-PT14-RERUN-V4.log`
- Report: `/opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb/working/W23A-PT14-RERUN-V4-REPORT.md`
