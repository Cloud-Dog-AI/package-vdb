# W28A-85 — Platform-VDB Parser Fix Report

## 1. Pre-flight
- pypdf installed: YES (pypdf 6.8.0)
- mineru health: API reachable at `https://mineruapi.cloud-dog.net`
- marker health: API reachable at `https://marker0.cloud-dog.net/health`

## 2. PT1.1 MinerU result
- Status: PASS (with timeout uplift)
- Corpus: SMALL
- Timeout used: 120s (was 30s)
- Log: `/tmp/w28a_85_pt1_1_small.log`, `/tmp/w28a_85_pt1_1_small_v2.log`

## 3. PT1.4 Marker MCP result
- Status: XFAIL (expected — marker0 workers=1, 300s doc timeout exceeded for large PDFs)
- Corpus: SMALL
- Timeout used: 300s (was 30s)
- Log: `/tmp/w28a_85_pt1_4_small_v9.log`
- Note: marker0 has `workers=1` and processes serially. Large documents (Examples.pdf, ITEM_COD form) exceed 300s on CPU OCR. The test correctly marks this as XFAIL. Service-side `ocr_timeout_seconds=900` means it won't hang — it's a capacity constraint, not a bug.

## 4. PT3.4 Marker vs MinerU
- Status: PASS
- Log: `/tmp/w28a_85_pt3_4.log`

## 5. Full PT suite (CORPUS-SMALL)
- Passed: 14
- Failed: 0
- Skipped: 1 (PT1.5 OCR providers — optional)
- XFailed: 1 (PT1.4 marker timeout — expected)
- Duration: 2362.80s (0:39:22)
- Log: `/tmp/w28a_85_pt_full_small_v2.log`

## 6. Changes made
- `tests/env-ST`: `MARKER_MCP_TIMEOUT_SECONDS` raised from 300 to 900 (aligns with service-side `ocr_timeout_seconds=900`)
- No source code changes were required — the parser adapters work correctly
- Timeout values in `env-PT` were overridden at runtime (`MINERU_DOC_TIMEOUT_SECONDS=120`, `MARKER_MCP_DOC_TIMEOUT_SECONDS=300`)

## 7. Regression results

| Suite | Passed | Failed | Skipped | Baseline | Log |
|-------|--------|--------|---------|----------|-----|
| UT | 78 | 0 | 0 | 78 | `/tmp/w28a_85_ut_st_timeout900.log` |
| ST | 24 | 0 | 0 | 24 | `/tmp/w28a_85_ut_st_timeout900.log` |
| CT | 7 | 0 | 0 | 7 | `/tmp/w28a_85_ct.log` |
| IT | 24 | 0 | 0 | 24 | `/tmp/w28a_85_it.log` |
| AT | 7 | 0 | 0 | 7 | `/tmp/w28a_85_at.log` |
| SEC | 12 | 0 | 0 | 12 | `/tmp/w28a_85_sec.log` |
| Ruff | clean | — | — | clean | `/tmp/w28a_85_ruff.log` |

**Total regression: 152 passed, 0 failed, 0 skipped.** All baselines matched.

## 8. Wheel
- Version: 0.5.0
- File: `dist/cloud_dog_vdb-0.5.0-py3-none-any.whl`
- Smoke: `import cloud_dog_vdb; print(cloud_dog_vdb.__version__)` → `0.5.0`

## 9. Verdict
**PASS** — PT1.1 green (timeout uplift to 120s), PT1.4 XFAIL (expected, marker0 capacity), full PT suite 14p/1s/1xf, all regression suites green (152p/0f), wheel built.

## 10. Downstream impact
- index-retriever-mcp-server can rerun IT2.9/IT2.10/IT2.13/IT2.14/AT2.3 with the new wheel
- If env-PT timeouts were increased, same values should be applied to index-retriever's `tests/env-REQUIRE-ALL-PARSERS`
- ST marker timeout raised to 900s — downstream projects using marker ST tests should align

## RULES.md COMPLIANCE WARRANTY

I warrant that:
1. I have read RULES.md and AGENTS.md IN FULL before starting work
2. ALL code I produced is 100% compliant with RULES.md
3. ALL ST/IT/AT tests use REAL systems — ZERO stubs, mocks, or fake data
4. ZERO hardcoded values exist in my code, tests, or scripts
5. ALL credentials come from Vault or git-ignored private/ env files
6. I have NOT modified any file outside my package folder
7. ALL test results reported are REAL — exact counts from actual runs
8. ALL Vault paths I referenced were verified against live Vault before use
