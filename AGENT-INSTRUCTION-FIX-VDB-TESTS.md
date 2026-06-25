# Agent Instruction — Fix cloud_dog_vdb Test Infrastructure

**Package:** `cloud_dog_vdb`
**Version:** 0.3.0
**Date:** 2026-02-20
**Status:** OPEN — CRITICAL defects in test infrastructure
**Scope:** Fix silent-skip test fraud, missing precondition checks, misclassified tests, env file gaps

---

## INTEGRITY WARRANTY — READ THIS FIRST

This section is copied verbatim from the platform-wide `RULES.md` Section 1. It is **NON-NEGOTIABLE**.

**I WILL NEVER:**
- **LIE** about test results, implementation status, or compliance
- **FUDGE** test data, configuration, or validation results
- **HACK** around problems instead of fixing root causes
- **FALSIFY** test outputs, logs, or status reports
- **STUB** functionality in IT/AT tests when real implementation is required
- **FAKE** success when there are errors, warnings, or failures
- **HIDE** failures, warnings, or non-compliance
- **PRETEND** tests pass when they fail
- **SKIP** validation steps to claim completion
- **BYPASS** rules or requirements for convenience

**IF I CANNOT GUARANTEE 100% COMPLIANCE, I WILL STOP AND SAY SO EXPLICITLY.**

**IF TESTS FAIL, I WILL REPORT FAILURES HONESTLY, NOT HIDE THEM.**

**IF I DON'T KNOW, I WILL ASK, NOT GUESS.**

**"ASK. DON'T GUESS. DON'T LIE. DON'T FUDGE."**

---

## ADDITIONAL RULES — ZERO TOLERANCE

These rules supplement the Integrity Warranty. Violation of ANY rule invalidates all work.

1. **100% REAL systems in IT/AT** — no mocks, no stubs, no `local_mode=True`, no `MockTransport`. If it says "Integration" it MUST integrate with a real external service.
2. **Silent skip is a LIE** — `pytest.skip()` when a backend is unavailable makes the test report say "0 failed". This is indistinguishable from "all passed". A skipped IT/AT test is NOT a passed test. It is an UNTESTED test.
3. **env files MUST be complete** — if a test tier requires `VAULT_TOKEN`, the env file for that tier MUST either contain it or the test MUST `pytest.fail()` (not skip) when it is absent.
4. **Test type MUST match reality** — a test that calls pure functions with no external service is a **UT**, not an IT/AT/QT. A test that uses `local_mode=True` is a **ST** at best, not an IT/AT.
5. **No decoration env files** — every variable in an env file MUST be consumed by the test code that loads it. If the test ignores the env file and hard-codes Vault, the env file is decoration and MUST be removed or the test MUST be fixed to use it.
6. **Write-path precondition checks** — before attempting backend writes in IT tests, probe the backend with a lightweight write-then-delete operation. If the probe fails, `pytest.fail()` with a clear message identifying the backend and error. Do NOT just crash with an opaque 500.
7. **Config delegation** — test fixtures MUST use the same config loading path as the application. If the app uses `cloud_dog_config` layered precedence, tests MUST NOT bypass it by shelling out to Vault directly.
8. **Honest reporting** — when reporting test results, ALWAYS state the skip count. "76 passed, 0 failed, 11 skipped" is NOT the same as "76 passed". If skipped tests include IT tests that should have run against real backends, this MUST be flagged as a gap, not hidden.

---

## WHY THIS INSTRUCTION EXISTS

### Audit Findings (2026-02-20)

An audit of the `cloud_dog_vdb` test infrastructure found **5 critical defects**:

| ID | Severity | Finding | Evidence |
|----|----------|---------|----------|
| **T-1** | CRITICAL | `env-IT` missing `VAULT_TOKEN` — all 11 real-backend IT tests silently skip | `tests/env-IT` has 3 of 4 required Vault vars. `conftest.py:59` calls `pytest.skip()` when `VAULT_TOKEN` is missing. Result: "0 failed, 11 skipped" reported as "PASS". |
| **T-2** | CRITICAL | 10 tests misclassified as IT/AT/QT when they are actually UT/ST | See § Misclassified Tests below. |
| **T-3** | HIGH | No write-path precondition check before backend operations | IT tests dive straight into `create_collection()` / `add_documents()`. When backend has resource exhaustion (e.g. Chroma FD limit), tests crash with opaque HTTP 500. |
| **T-4** | HIGH | `pytest.skip()` used instead of `pytest.fail()` for mandatory IT preconditions | `conftest.py:59` and all 11 IT tests use `pytest.skip()` when config/backend is missing. For IT tests that MUST run against real backends, this should be `pytest.fail()`. |
| **T-5** | MEDIUM | `env-UT`, `env-ST`, `env-AT` all contain identical Vault vars but no `VAULT_TOKEN` | All 4 env files have `VAULT_ADDR`, `VAULT_MOUNT_POINT`, `VAULT_CONFIG_PATH` but not `VAULT_TOKEN`. UT/ST don't need Vault (per RULES.md § 5.5). AT tests use `local_mode=True` so don't need it either. These Vault vars are decoration. |

### Misclassified Tests (T-2)

| Test | Claimed Type | Actual Type | Evidence |
|------|-------------|-------------|----------|
| IT1.11 CrossBackendPortable | Integration | ST (local) | Uses `local_mode=True` — no real backend |
| IT1.13 LifecycleRealBackend | Integration | **UT** | Calls `mark_deleted()` / `mark_superseded()` — pure functions, no backend |
| AT1.1 ServiceStartupPattern | Application | ST (local) | Uses `local_mode=True` — no real service |
| AT1.2 FullIngestionFlow | Application | ST (local) | Uses `local_mode=True` — no real service |
| AT1.3 SearchWithFilters | Application | ST (local) | Uses `local_mode=True` — no real service |
| AT1.4 ConformanceSuite | Application | **UT** | Uses `mock_adapter()` — mock, not real |
| AT1.5 ClientOnlyIntegration | Application | **UT** | Uses `httpx.MockTransport` — mock, not real |
| QT1.1 TenantIsolation | Security | **UT** | Calls `enforce_tenant()` — pure function, 7 lines |
| QT1.2 AccessControlEnforcement | Security | **UT** | Calls `can_admin()` — pure function, 7 lines |
| QT1.4 PurgeRequiresAdmin | Security | **UT** | Calls `can_admin()` — pure function, 8 lines |

**Correctly classified tests:** QT1.3 (static file scan — legitimate QT).

---

## HARD CONSTRAINTS

- **DO NOT** delete any test. Reclassify by moving to the correct directory.
- **DO NOT** add `os.environ`/`os.getenv` reads in library source code.
- **DO NOT** weaken any existing test assertion.
- **DO NOT** convert real-backend IT tests to use `local_mode=True`.
- **DO NOT** claim completion without running the full verification chain AND reporting skip counts.
- **UK English only.**

---

## PHASE 1 — Fix env-IT to include VAULT_TOKEN (T-1)

### Step 1.1 — Decide: env-IT should NOT contain VAULT_TOKEN directly

Per RULES.md § 5.5: *"NEVER save keys, passwords, tokens, or credentials into the repository."*

`VAULT_TOKEN` is a credential. It MUST NOT be in `env-IT`.

**Solution:** The test runner MUST source `env-vault` before running IT tests. The `conftest.py` fixture must enforce this by calling `pytest.fail()` (not `pytest.skip()`) when `VAULT_TOKEN` is missing for IT-tier tests.

### Step 1.2 — Update `conftest.py`: fail instead of skip for IT/AT tests

Change `conftest.py` `vault_config()` fixture:

```python
# BEFORE:
if missing:
    pytest.skip(f"Vault variables missing: {', '.join(missing)}")

# AFTER:
if missing:
    tier = os.environ.get("TEST_ENV_TIER", "")
    if tier in ("IT", "AT"):
        pytest.fail(
            f"VAULT_TOKEN and Vault variables are REQUIRED for {tier} tests. "
            f"Missing: {', '.join(missing)}. "
            f"Run: set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a"
        )
    pytest.skip(f"Vault variables missing (UT/ST may skip): {', '.join(missing)}")
```

### Step 1.3 — Add `TEST_ENV_TIER` to env files

Add to each env file:

| File | Add |
|------|-----|
| `env-UT` | `TEST_ENV_TIER=UT` |
| `env-ST` | `TEST_ENV_TIER=ST` |
| `env-IT` | `TEST_ENV_TIER=IT` |
| `env-AT` | `TEST_ENV_TIER=AT` |

### Step 1.4 — Remove decoration Vault vars from env-UT and env-ST

UT and ST tests MUST NOT require Vault (RULES.md § 5.5). Remove `VAULT_ADDR`, `VAULT_MOUNT_POINT`, `VAULT_CONFIG_PATH` from `env-UT` and `env-ST`.

---

## PHASE 2 — Add write-path precondition checks (T-3)

### Step 2.1 — Create a reusable backend probe fixture

Add to `conftest.py`:

```python
@pytest.fixture(scope="session")
def chroma_ready(vdbs: dict) -> dict:
    """Verify Chroma can actually handle writes, not just heartbeat."""
    cfg = vdbs.get("chroma", {})
    if not cfg:
        pytest.fail("dev.vdbs.chroma missing from Vault config")
    from cloud_dog_vdb.adapters.chroma import ChromaAdapter
    from cloud_dog_vdb.config.models import ProviderConfig
    from cloud_dog_vdb.domain.models import CollectionSpec
    import asyncio

    a = ChromaAdapter(
        ProviderConfig(provider_id="chroma", base_url=cfg.get("base_url", ""), api_key=cfg.get("auth_token", "")),
        local_mode=False,
    )
    probe_name = "_cloud_dog_write_probe"
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(a.delete_collection(probe_name))
        loop.run_until_complete(a.create_collection(CollectionSpec(name=probe_name)))
        loop.run_until_complete(a.delete_collection(probe_name))
    except Exception as exc:
        pytest.fail(f"Chroma write-path probe FAILED: {exc}")
    return cfg
```

Create equivalent `qdrant_ready`, `weaviate_ready`, `opensearch_ready`, `pgvector_ready` fixtures.

### Step 2.2 — Update IT tests to use `*_ready` fixtures instead of raw `vdbs`

Replace `vdbs` parameter with the appropriate `*_ready` fixture in each IT test.

---

## PHASE 3 — Reclassify misclassified tests (T-2)

### Step 3.1 — Move misclassified tests to correct directories

| Current Location | Move To | Reason |
|-----------------|---------|--------|
| `integration/IT1.11_CrossBackendPortable/` | `system/ST1.9_CrossBackendPortableLocal/` | Uses `local_mode=True` |
| `integration/IT1.13_LifecycleRealBackend/` | `unit/UT1.38_LifecycleFunctions/` | Pure function calls |
| `application/AT1.1_ServiceStartupPattern/` | `system/ST1.10_ServiceStartupLocal/` | Uses `local_mode=True` |
| `application/AT1.2_FullIngestionFlow/` | `system/ST1.11_FullIngestionLocal/` | Uses `local_mode=True` |
| `application/AT1.3_SearchWithFilters/` | `system/ST1.12_SearchWithFiltersLocal/` | Uses `local_mode=True` |
| `application/AT1.4_ConformanceSuite/` | `unit/UT1.39_ConformanceMock/` | Uses `mock_adapter()` |
| `application/AT1.5_ClientOnlyIntegration/` | `unit/UT1.40_RemoteClientMock/` | Uses `MockTransport` |
| `security/QT1.1_TenantIsolation/` | `unit/UT1.41_TenantIsolationLogic/` | Pure function |
| `security/QT1.2_AccessControlEnforcement/` | `unit/UT1.42_AccessControlLogic/` | Pure function |
| `security/QT1.4_PurgeRequiresAdmin/` | `unit/UT1.43_PurgeAdminLogic/` | Pure function |

### Step 3.2 — Write REAL IT/AT/QT replacements

For each reclassified test, write a NEW test at the original location that actually uses real backends:

| New Test | What It Must Do |
|----------|----------------|
| IT1.11 CrossBackendPortable | Run the same portable contract against BOTH Chroma AND Qdrant real backends |
| IT1.13 LifecycleRealBackend | Mark records as deleted/superseded IN a real Chroma collection, verify via query |
| AT1.1 ServiceStartupPattern | Start a real VDB client against Chroma, verify `init_backend()` succeeds against real server |
| AT1.2 FullIngestionFlow | Run full ingestion against real Chroma, not `local_mode` |
| AT1.3 SearchWithFilters | Search with metadata filters against real Chroma |
| AT1.4 ConformanceSuite | Run `adapter_conforms()` against a REAL adapter, not mock |
| AT1.5 ClientOnlyIntegration | If `RemoteVDBClient` is meant for real remote use, test against a real endpoint. If no real endpoint exists, document this as a gap and leave it as UT until a remote VDB service is deployed. |
| QT1.1 TenantIsolation | Ingest records with different `tenant_id` into real Chroma, verify isolation via search |
| QT1.2 AccessControlEnforcement | Test access control against a real adapter operation |
| QT1.4 PurgeRequiresAdmin | Attempt purge on a real collection, verify admin enforcement |

---

## PHASE 4 — Clean up env files (T-5)

### Step 4.1 — env-UT contents

```
TEST_ENV_TIER=UT
```

UT tests MUST NOT need anything else. If they do, the test has a dependency problem.

### Step 4.2 — env-ST contents

```
TEST_ENV_TIER=ST
```

ST tests use `local_mode=True` and in-memory backends. No external config needed.

### Step 4.3 — env-IT contents

```
TEST_ENV_TIER=IT
VAULT_ADDR=https://vault0.cloud-dog.net
VAULT_MOUNT_POINT=cloud_dog_ai
VAULT_CONFIG_PATH=config
```

`VAULT_TOKEN` comes from `env-vault` sourced before test run. The conftest will `pytest.fail()` if it is missing.

### Step 4.4 — env-AT contents

```
TEST_ENV_TIER=AT
VAULT_ADDR=https://vault0.cloud-dog.net
VAULT_MOUNT_POINT=cloud_dog_ai
VAULT_CONFIG_PATH=config
```

AT tests require real backends. Same Vault dependency as IT.

---

## PHASE 5 — Update TESTS.md

### Step 5.1 — Update test directory structure to reflect reclassifications

### Step 5.2 — Update test counts

After reclassification, counts should be approximately:
- **UT:** 37 (original) + 6 (reclassified from IT/AT/QT) = **43**
- **ST:** 8 (original) + 4 (reclassified from IT/AT) = **12**
- **IT:** 13 (original) - 2 (reclassified) + 2 (new real replacements) = **13**
- **AT:** 5 (original) - 5 (reclassified) + 5 (new real replacements) = **5**
- **QT:** 4 (original) - 3 (reclassified) + 3 (new real replacements) = **4**

### Step 5.3 — Update Test Run History

Record actual results with skip counts. Example:

```
| Date | Scope | Command | Passed | Failed | Skipped | Notes |
```

**NEVER write "PASS" without the skip count.**

---

## PHASE 6 — Verification

### Step 6.1 — Run UT + ST (no Vault required)

```bash
cd /opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb
.venv/bin/pytest tests/unit tests/system --env tests/env-UT --env tests/env-ST -v
```

**Expected:** All pass, 0 skipped.

### Step 6.2 — Run IT + AT (Vault required)

```bash
set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a
.venv/bin/pytest tests/integration tests/application --env tests/env-IT --env tests/env-AT -v
```

**Expected:** All pass against available backends. Any backend that is down should produce `pytest.fail()` with a clear error message, NOT a silent skip.

### Step 6.3 — Run QT (Vault required for real-backend QT tests)

```bash
.venv/bin/pytest tests/security --env tests/env-IT -v
```

### Step 6.4 — Report honestly

State exact counts: `N passed, N failed, N skipped`. If any IT/AT test skipped, explain WHY and which backend is unavailable.

---

## COMPLETION GATE

This instruction is complete ONLY when:

1. `env-IT` no longer causes silent skips — missing `VAULT_TOKEN` produces `pytest.fail()`
2. All 10 misclassified tests are moved to correct directories
3. Real-backend replacements exist for all reclassified IT/AT/QT tests
4. Write-path probe fixtures exist for all 5 backends
5. TESTS.md updated with correct classifications and honest run history
6. Full test suite runs with `0 skipped` for UT/ST tier
7. IT/AT tier runs against real backends with honest reporting

**DO NOT claim completion without evidence for ALL 7 gates.**
