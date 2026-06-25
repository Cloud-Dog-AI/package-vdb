# W28A-116-FIX-VDB Report

Date (UTC): 2026-03-11
Package: `packages/backend/platform-vdb`
Instruction: `working/AGENT-INSTRUCTION-W28A-116-FIX-VDB.md`

## 1. Run Summary

### Files changed
- `cloud_dog_vdb/testing/comparison.py`
  - Removed direct `os.environ` reads from package source.
  - Replaced env-driven timeout/health lookups with config-driven lookups from provider config (`doc_timeout_*`, `health_*`).
- `tests/parser/_comparison_helpers.py`
  - Added env-to-config override mapping for comparison tests.
  - Injects timeout/health override values into provider config passed into `CrossProviderComparison`.
- `tests/conftest.py`
  - Reformatted using Ruff to satisfy `ruff format --check`.

### Tests fixed and how
- Config delegation grep gate was failing due `os.environ` usage in `cloud_dog_vdb/testing/comparison.py`.
  - Fixed by removing env access from package source and moving env mapping into test helper.
- Ruff gate was failing due formatting drift in `tests/conftest.py`.
  - Fixed by running `ruff format tests/conftest.py` and re-running Ruff checks.

## 2. Test Results

- QT: `12p / 0f`
- UT: `78p / 0f`
- ST: `24p / 0f`
- IT: `24p / 0f`
- AT: `7p / 0f`
- Ruff: `0 issues` (check + format-check both pass)

Additional verification:
- Config grep (`cloud_dog_vdb/`): `ZERO_MATCH`
- `cloud_dog_vdb/secrets` directory: `PASS` (not present)
- Build: success (`cloud_dog_vdb-0.5.0.tar.gz`, `cloud_dog_vdb-0.5.0-py3-none-any.whl`)

## 3. Verdict

**PASS**

## 4. Evidence Logs

- `working/w28a-116-qt.log`
- `working/w28a-116-ut.log`
- `working/w28a-116-st.log`
- `working/w28a-116-it.log`
- `working/w28a-116-at.log`
- `working/w28a-116-ruff.log`
- `working/w28a-116-config-grep.log`
- `working/w28a-116-secrets-dir.log`
- `working/w28a-116-build.log`

## 5. RULES.md COMPLIANCE WARRANTY

I warrant that:
1. I have read RULES.md IN FULL before starting work
2. ALL code I produced is 100% compliant with RULES.md
3. ALL test results reported are REAL — exact counts from actual runs
4. I have NOT weakened any test
5. I have NOT stored, copied, or exposed any credentials
6. ALL credentials come from Vault or git-ignored env files
7. I have NOT modified files outside this package

---

## Addendum (2026-03-11 UTC) — MinerU Fail-Fast + Stability Closure

### Root cause found
- MinerU parser failures were caused by endpoint route instability/unavailability (`404 page not found`) observed at:
  - `https://mineruapi.cloud-dog.net/file_parse`
  - `https://minerugui.cloud-dog.net/gradio_api/upload`
- Live evidence captured in `working/w28a-116-mineru-curl-watch.log`.

### Code changes applied
- `cloud_dog_vdb/ingestion/parse/providers/mineru.py`
  - Added explicit route-not-found detection for persistent `404` responses.
  - Added fail-fast error path: raises `InvalidRequestError` with endpoint-unavailable diagnostics instead of spending time in page-fallback loops.
  - Tightened `health_check()` accepted status handling to avoid treating `404` as healthy.
- `tests/unit/UT2.12_ProviderHealthContract/test_provider_health_contract.py`
  - Added regression test `test_ut2_12_mineru_fail_fast_on_persistent_route_404`.

### Verification results (fresh runs)
- QT: `12 passed` (`working/w28a-116-qt.log`)
- UT: `81 passed` (`working/w28a-116-ut.log`)
- ST: `24 passed` (`working/w28a-116-st.log`)
- IT: `24 passed` (`working/w28a-116-it.log`)
- AT: `7 passed` (`working/w28a-116-at.log`)
- PT (MinerU corpus matrix): `1 passed` (`working/w28a-116-pt11.log`)

### Diagnostic traces retained
- Fail-fast failure evidence when endpoints returned 404:
  - `working/w28a-116-st2-postfix.log` (`2 failed in 14.91s`)
- Post-recovery rerun evidence:
  - `working/w28a-116-st2-rerun-after-fix.log` (`2 passed`)
  - `working/w28a-116-it27-rerun.log` (`1 passed`)
  - `working/w28a-116-it211.log` (`1 passed`)
  - `working/w28a-116-at22.log` (`1 passed`)
