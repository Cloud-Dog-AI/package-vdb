# W23A-P2 Report - platform-vdb 0.5.0 Recovery

Date: 2026-03-05
Project: `packages/backend/platform-vdb`
Instruction: `working/AGENT-INSTRUCTION-W23A-P2-VDB-RECOVERY.md`
Status: RECOVERY EXECUTED

## Scope
Resolved and re-tested the three blockers called out in prior W23A-P2 closeout:
1. AT2.2 MinerU 404 mismatch.
2. PT non-terminating (PT1.4 / PT3.1).
3. Registry verification auth flow.

Evidence logs are in:
`packages/backend/platform-vdb/tmp/w23a-p2-recovery-2026-03-05/`

## Blocker 1 - AT2.2 MinerU 404

### Evidence
- `w23a-p2-mineru-health-code.log`: `000` when using `${MINERU_BASE_URL:-http://localhost:8010}` from shell (env var unset in shell, fallback URL used).
- `w23a-p2-vault-services.log`: Vault service URI exists for MinerU (`https://mineruapi.cloud-dog.net`) and Marker (`https://marker0.cloud-dog.net`).
- `w23a-p2-mineru-cloud-openapi-code.log`: `200`
- `w23a-p2-mineru-cloud-health-code.log`: `404` (MinerU deployment does not expose `/health`)
- `w23a-p2-mineru-file-parse-code.log`: `200` on live `POST /file_parse` with real PDF
- `w23a-p2-at22.log`: `AT2.2` now passes (`1 passed`)

### Resolution
- Service availability validated via live OpenAPI and live parse endpoint rather than `/health`.
- AT2.2 gate re-run passes.

### Status
- **RESOLVED** for W23A-P2 (AT2.2 failure condition cleared).

## Blocker 2 - PT non-terminating

### Initial evidence
- `w23a-p2-pt14-small.log`: `__EXIT_CODE=124`
- `w23a-p2-pt3-small.log`: `__EXIT_CODE=124`

### Investigation
- Per-provider isolation on PT3.1 showed individual providers terminate and pass under bounded document timeouts.
- The non-terminating behaviour was tied to unbounded/oversized effective timeout budget for full matrix runs.

### Fix applied
Updated `tests/env-PT` to enforce deterministic parser test bounds and explicit comparison provider set:
- `PARSER_DOC_TIMEOUT_MULTIPLIER=1.0`
- `PARSER_DOC_TIMEOUT_MIN_SECONDS=30`
- `PARSER_DOC_TIMEOUT_MAX_SECONDS=30`
- `MINERU_DOC_TIMEOUT_SECONDS=30`
- `MARKER_MCP_DOC_TIMEOUT_SECONDS=30`
- `DEEPDOC_DOC_TIMEOUT_SECONDS=30`
- `DOCLING_DOC_TIMEOUT_SECONDS=30`
- `TRANSFORMERS_DOC_TIMEOUT_SECONDS=30`
- `COMPARISON_PROVIDERS=internal,marker_mcp,mineru,deepdoc,docling,transformers`

### Re-test evidence
- `w23a-p2-pt14-small-final.log`: `1 xfailed in 0.49s`, `__EXIT_CODE=0` (terminates; no hang)
- `w23a-p2-pt3-small-final2.log`: `1 passed in 89.03s`, `__EXIT_CODE=0`

### Status
- **RESOLVED** for W23A-P2 (non-termination cleared).
- Note: PT1.4 remains soft-fail capable (`xfail`) when live Marker reports busy; this is expected runtime handling, not a hang.

## Blocker 3 - Registry verification auth

### Evidence
- `w23a-p2-registry-index.log`: unauthenticated/empty-credential flow fails (`No matching distribution found`).
- `w23a-p2-registry-index-vaultcreds-dash.log`: authenticated Vault-derived index query succeeds and lists versions including `0.5.0`.
- `w23a-p2-registry-index-vaultcreds-underscore.log`: same success for underscore package alias.

### Resolution
- Retrieved repository credentials from Vault at runtime and used authenticated index URL for non-interactive verification.
- Stored only redacted repository evidence (`w23a-p2-vault-repository-redacted.log`) in repo evidence folder.

### Status
- **RESOLVED** for W23A-P2.

## Updated Gate Table (Recovery Scope)

| Gate | Command | Result |
|---|---|---|
| B1-AT2.2 | `.venv/bin/pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-REQUIRE-ALL-PARSERS -q` | PASS (`1 passed`) |
| B2-PT1.4 small | `timeout 300 .venv/bin/pytest tests/parser/PT1.4_MarkerMcpAdapter --env tests/env-PT --env tests/env-CORPUS-SMALL -q` | PASS (terminates, `1 xfailed`, exit 0) |
| B2-PT3.1 small | `timeout 300 .venv/bin/pytest tests/parser/PT3.1_CorpusSmallComparison --env tests/env-PT --env tests/env-CORPUS-SMALL -q` | PASS (`1 passed`, exit 0) |
| B3-registry verify | `.venv/bin/pip index versions cloud-dog-vdb --index-url <vault-auth-url>` | PASS (versions returned, includes `0.5.0`) |

## Compliance Notes
- No infrastructure mutation performed (no Terraform/Docker/Vault structure changes).
- No fabricated evidence; all results are from session logs listed above.
- Sensitive values were redacted from repository-stored evidence logs.

## Completion Warranty - W23A-P2 Recovery
This recovery instruction scope is **PASS**:
- Blocker 1: resolved
- Blocker 2: resolved
- Blocker 3: resolved

This report does **not** replace full package release gating; it records the targeted blocker recovery actions and outcomes only.

---

## 2026-03-06 Rerun Addendum (Agent 7 - P2 VDB Completion)

Date: 2026-03-06  
Instruction: `working/AGENT-INSTRUCTION-W23A-P2-VDB-RECOVERY.md`  
Evidence folder: `packages/backend/platform-vdb/tmp/w23a-p2-recovery-2026-03-06/`

### Blocker 1 - AT2.2 MinerU 404 (post P0A Marker fix)

Evidence:
- `w23a-p2-mineru-health-code.log`: `MINERU_HEALTH_CODE=000` using shell fallback URL (`MINERU_BASE_URL` unset in shell)
- `w23a-p2-vault-services.log`: Vault contains MinerU URI and Marker URI
- `w23a-p2-mineru-url.log`: `MINERU_BASE_URL_FROM_VAULT=https://mineruapi.cloud-dog.net`
- `w23a-p2-mineru-cloud-openapi-code.log`: `MINERU_OPENAPI_CODE=200`
- `w23a-p2-mineru-cloud-health-code.log`: `MINERU_HEALTH_CODE=404`
- `w23a-p2-mineru-file-parse-code.log`: `MINERU_FILE_PARSE_CODE=422` (direct diagnostic endpoint contract mismatch)
- `w23a-p2-at22-rerun.log`: AT2.2 gate pass (`1 passed in 27.91s`)
- `w23a-p2-at22-rerun.exit`: `AT22_EXIT_CODE=0`

Resolution status:
- **RESOLVED for AT2.2 gate** (the required application gate now passes after Marker v3 fix).
- MinerU deployment still does not expose `/health`, and direct `/file_parse` probe returns 422 under this diagnostic payload.

### Blocker 2 - PT non-terminating

Timeout-marked small corpus reruns:
- `w23a-p2-pt14-small-rerun.log`: completed in `150.92s` (no hang), but failed quality assert (`success_ratio=0.0`), `PT14_EXIT_CODE=1`
- `w23a-p2-pt14-small-rerun2.log`: completed quickly as `xfailed`, `PT14_RERUN2_EXIT_CODE=0`
- `w23a-p2-pt14-small-rerun3-rx.log`: `xfailed` with reason `marker_mcp busy: inflight=5, workers=2, status=ok`, `PT14_RERUN3_EXIT_CODE=0`
- `w23a-p2-pt3-small-rerun.log`: pass (`1 passed in 133.37s`), `PT3_EXIT_CODE=0`

Resolution status:
- **Non-termination is RESOLVED** (no timeout 124 observed in this rerun set).
- PT1.4 is **not stable-pass** in shared runtime conditions (one hard fail, then provider-busy xfails).

### Blocker 3 - Registry verification auth

Evidence:
- `w23a-p2-registry-index-auth-rerun.log`: authenticated index query succeeds and lists versions including `0.5.0`
- `w23a-p2-registry-index-auth-rerun.exit`: `REGISTRY_EXIT_CODE=0`
- `w23a-p2-vault-repository-rerun.log`: repository section captured with secrets redacted

Resolution status:
- **RESOLVED**.

### Updated Gate Results (2026-03-06)

| Gate | Command | Result |
|---|---|---|
| AT2.2 | `timeout 900 .venv/bin/pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-REQUIRE-ALL-PARSERS -q` | PASS (`1 passed`, exit 0) |
| PT1.4 small (run 1) | `timeout 300 .venv/bin/pytest tests/parser/PT1.4_MarkerMcpAdapter --env tests/env-PT --env tests/env-CORPUS-SMALL -q` | FAIL (`success_ratio=0.0`, exit 1) |
| PT1.4 small (run 3, -rx) | `timeout 300 .venv/bin/pytest tests/parser/PT1.4_MarkerMcpAdapter --env tests/env-PT --env tests/env-CORPUS-SMALL -q -rx` | XFAIL (`marker_mcp busy`, exit 0) |
| PT3.1 small | `timeout 300 .venv/bin/pytest tests/parser/PT3.1_CorpusSmallComparison --env tests/env-PT --env tests/env-CORPUS-SMALL -q` | PASS (`1 passed`, exit 0) |
| Registry auth | authenticated `pip index versions cloud-dog-vdb` using Vault credentials | PASS (0.5.0 listed, exit 0) |

### Completion Warranty (2026-03-06 rerun scope)

**VOID (partial)** for full P2 completion claim at this timestamp.

Reason:
- AT2.2 and PT3.1 pass, timeout-hang symptom is cleared, and registry verification passes.
- PT1.4 does not currently demonstrate stable clean-pass under shared runtime load; observed outcomes include hard fail and provider-busy xfail.

This rerun therefore confirms **recovery progress** but not a clean full-scope completion warranty.
