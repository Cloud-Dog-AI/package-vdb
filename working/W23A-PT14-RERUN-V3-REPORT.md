# W23A PT1.4 Re-run V3 Report

- Date (UTC): 2026-03-06T13:15:40Z
- Project: `platform-vdb`
- Scope: PT1.4 rerun against marker0 v4 stability rollout
- Change policy: No source code, test files, or env files modified

## Phase 1: marker0 health before run

Requested command:

```bash
curl -sf https://marker0.cloud-dog.net/health | python3 -m json.tool
```

Observed:
- Command returned JSON parse error (`Expecting value`) in this shell path.
- Direct raw health check succeeded and showed healthy/not busy state:

```json
{"status":"ok","busy":false,"inflight_requests":0,"workers":1,"ocr_queue_wait_seconds":30.0,"ocr_timeout_seconds":900.0,"uptime_seconds":1372.3}
```

## Phase 2: PT1.4 run

Requested command:

```bash
cd /opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb
set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a
timeout 1800 python3 -m pytest tests/parser/PT1.4_MarkerMcpAdapter/ \
  --env-file tests/env-PT -x -v -s 2>&1 | tee working/W23A-PT14-RERUN-V3.log
```

Observed:
- `--env-file` is not recognized by this pytest setup (immediate CLI error).
- Re-run executed with supported flag `--env tests/env-PT` to apply `tests/env-PT` settings.
- Run started, collected 1 test, then produced no further test output.
- `timeout 1800` boundary hit (`PIPE_EXIT=124`).

Log evidence:
- `working/W23A-PT14-RERUN-V3.log` contains only session header + test identifier (9 lines), no final pytest summary.

## Phase 3: soft-fail checks and post-run health

Soft-fail grep command:

```bash
cat working/W23A-PT14-RERUN-V3.log | grep -E "xfail|XFAIL|busy|unhealthy|soft_fail"
```

Observed:
- No matches.

Post-run health:
- Requested pretty-print command again returned JSON parse error in this shell path.
- Direct raw health check succeeded:

```json
{"status":"ok","busy":false,"inflight_requests":0,"workers":1,"ocr_queue_wait_seconds":30.0,"ocr_timeout_seconds":900.0,"uptime_seconds":3375.1}
```

## Required result fields

- Pass/fail/xfail result: **FAIL (timeout/inconclusive)**
  - No pass/fail/xfail emitted by pytest before timeout.
- marker0 health state before: **ok, busy=false, workers=1**
- marker0 health state after: **ok, busy=false, workers=1**
- `success_ratio`: **not emitted in output**
- `quality_invariant_pass_rate`: **not emitted in output**

## Artifacts

- Test log: `/opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb/working/W23A-PT14-RERUN-V3.log`
- Report: `/opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb/working/W23A-PT14-RERUN-V3-REPORT.md`
