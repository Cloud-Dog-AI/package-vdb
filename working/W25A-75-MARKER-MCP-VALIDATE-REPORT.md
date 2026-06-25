# W25A-75 — Marker MCP Transport Validation Report

## 1. Summary
**PASS** — Marker MCP JSON-RPC transport validated on `/mcp`, full gate set completed with real systems, wheel build/install/smoke passed (`0.5.0`).

## 2. Marker0 Health Check Output
### Step 0 pre-flight (`/health`)
```json
{
  "status": "ok",
  "server": "mcp",
  "busy": false,
  "inflight_requests": 0,
  "workers": 1,
  "ocr_queue_wait_seconds": 30.0,
  "ocr_timeout_seconds": 900.0,
  "uptime_seconds": 931.2
}
```

### Step 0 MCP initialize probe
```text
HTTP/1.1 200 OK
Content-Type: text/event-stream
Mcp-Session-Id: 36f3e9fb66a647198eb19ebd74eca379

event: message
data: {"jsonrpc":"2.0","id":1,"result":{..."serverInfo":{"name":"marker-mcp","version":"1.26.0"}}}
```

### Post-run health snapshot (2026-03-08T15:31:44Z)
```json
{
  "status": "ok",
  "server": "mcp",
  "busy": false,
  "inflight_requests": 0,
  "workers": 1,
  "ocr_queue_wait_seconds": 30.0,
  "ocr_timeout_seconds": 900.0,
  "uptime_seconds": 3323.7
}
```

## 3. Test Results Matrix
| Suite | Command | Passed | Failed | Skipped | Blocker |
|-------|---------|--------|--------|---------|---------|
| UT | `.venv/bin/pytest tests/unit --env tests/env-UT -v --tb=short` | 78 | 0 | 0 | None |
| ST | `pytest tests/system` executed as deterministic split: ST1+ST2.1..2.7, ST2.8, ST3.1, ST3.2, ST3.3, ST3.4 | 24 | 0 | 0 | None (transient queue-busy on first ST3.1 attempt, cleared on rerun) |
| CT | `.venv/bin/pytest tests/compatibility --env tests/env-CT -v --tb=short` | 7 | 0 | 0 | None |
| IT | `pytest tests/integration` plus isolated tail rerun (`IT2.7`, `IT2.8`, `IT2.9`) after long-tail stall | 24 | 0 | 0 | None |
| AT | `.venv/bin/pytest tests/application --env tests/env-AT -v --tb=short` | 7 | 0 | 0 | None |
| PT | `.venv/bin/pytest tests/parser/PT1.2_DeepdocAdapter tests/parser/PT1.3_DoclingAdapter tests/parser/PT1.8_TransformersAdapter --env tests/env-PT --env tests/env-CORPUS-SMALL -v --tb=short` | 3 | 0 | 0 | None |
| SEC | `.venv/bin/pytest tests/security --env tests/env-IT -v --tb=short` | 12 | 0 | 0 | None |

Lint/build gates:
- `ruff check`: pass
- `ruff format --check`: initial fail on `UT3.8` file, corrected by formatting, recheck pass

## 4. Build Result
- Command: `.venv/bin/python -m build`
- Artifacts:
  - `dist/cloud_dog_vdb-0.5.0-py3-none-any.whl` (94,765 bytes)
  - `dist/cloud_dog_vdb-0.5.0.tar.gz` (37,358,584 bytes)

## 5. Smoke Import
Commands:
- `.venv/bin/pip install --force-reinstall dist/cloud_dog_vdb-0.5.0-py3-none-any.whl`
- `.venv/bin/python -c "import cloud_dog_vdb; print(cloud_dog_vdb.__version__)"`

Output:
```text
0.5.0
```

## 6. Marker MCP Transport Verification
Evidence confirms MCP JSON-RPC transport path (`/mcp`) with `initialize`, `notifications/initialized`, and `tools/call`:

- Direct pre-flight initialize probe returned `200` with `Mcp-Session-Id` and JSON-RPC result.
- Direct transport probe (`tmp/w25a_75_marker_mcp_probe.log`) captured:
  - `POST /mcp method=initialize` -> `200`
  - `POST /mcp method=notifications/initialized` -> `202`
  - `POST /mcp method=tools/call name=marker_convert_pdf_base64` -> `200` (`text/event-stream`), returning MCP result envelope.
- ST3 live tests executed `MarkerMcpParserProvider.parse_bytes(...)` across sync, async-fallback, image artefact, and large-document flows, all passing on final reruns.

## 7. Blockers
No open blockers.

Observed transient issues during execution (resolved within this run):
- ST3.1 initial run failed with `marker_mcp parse failed: OCR worker busy; queue wait exceeded 30s` (live queue pressure); rerun passed.
- ST3.2 and IT2.7 were long-running with delayed output; both passed under controlled timeout reruns (`~3m49s` and `~2m46s` respectively).
- `ruff format --check` initially failed on one file; corrected and rechecked clean.

## 8. RULES.md COMPLIANCE WARRANTY
I warrant that:
1. I have read RULES.md IN FULL before starting work
2. ALL code I produced is 100% compliant with EVERY section of RULES.md
3. ALL tests I produced or modified are 100% compliant with RULES.md § 5
4. ALL ST/IT/AT tests use REAL systems — ZERO stubs, mocks, or fake data (§ 5.5)
5. ZERO hardcoded values exist in my code, tests, or scripts (§ 2.4)
6. ALL credentials come from Vault or git-ignored private/ env files — ZERO stored credentials (§ 2.3, § 9.2)
7. I have NOT modified any file outside my project folder (§ 9.1)
8. I have NOT accessed any server not explicitly provided (§ 9.3)
9. I have NOT stored, copied, or exposed any credentials (§ 9.2)
10. ALL test results reported are REAL — exact pass/fail/skip counts from actual runs
11. I have NOT modified any infrastructure file (Vault config, Terraform, deployment manifests) without explicit instruction (§ 10)
12. ALL Vault paths I referenced were verified against live Vault before use (§ 11)
13. ALL requirements I claimed as "implemented" have working code and passing tests — no stubs, no placeholders (§ 12)
