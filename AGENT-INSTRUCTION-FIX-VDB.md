# Agent Instruction — Fix cloud_dog_vdb (v0.2.0)

**Package:** `cloud_dog_vdb`
**Target version:** 0.2.0
**Date:** 2026-02-18 (re-review with source verification)
**Scope:** Config-delegation enforcement + adapter rewiring + test alignment + v0.2.0 features — **ALL DELIVERED AND VERIFIED**

---

## Status: ✅ COMPLETE (minor SA1 extra-files gap noted)

All 6 issues from the original instruction have been resolved. This document is retained for reference and future maintenance.

**Verified on 2026-02-18 (re-review):**
- 91 source files across 20+ subpackages
- 67 test directories present (37 UT + 8 ST + 13 IT + 5 AT + 4 QT)
- Zero config-delegation violations: `os.environ`/`hvac`/`overlay_secrets`/`VAULT_JSON` grep returns zero hits
- `secrets/` directory does NOT exist (deleted)
- All 5 adapters use `self.config.*` directly (no `self._runtime`, no overlay)
- Duplicate converter files removed (only `*_conv.py` versions remain)
- Test directories renamed to match TESTS.md v0.2.0 (ConfigDelegation, ConfigDelegationVerification, ConfigDelegationE2E)
- Old scaffold IT directories removed
- All 3 v0.2.0 feature files present and substantive
- Build produces `cloud_dog_vdb-0.2.0` wheel + sdist

**Governing documents:**
1. `platform-vdb/REQUIREMENTS.md` (v0.2.0) — FR1.31, FR1.33, FR1.34–FR1.36
2. `platform-vdb/ARCHITECTURE.md` (v0.2.0) — SA1 module layout
3. `platform-vdb/TESTS.md` (v0.2.0) — all test directories
4. `packages/backend/AGENT-INSTRUCTION.md` — Integrity Warranty and Config Delegation — ZERO TOLERANCE (MANDATORY)

---

## Delivery Summary

### Issue 1 — Config Delegation Enforcement ✅ RESOLVED

| Sub-issue | Status | Evidence |
|-----------|--------|----------|
| 1A. Delete `secrets/` module | ✅ | Directory does not exist |
| 1B. Rewire all 5 adapters | ✅ | Zero `overlay_secrets`, `self._runtime`, or `from cloud_dog_vdb.secrets` hits in source |
| 1C. Fix `observability/otel.py` | ✅ | Zero `os.environ` hits in any source file |
| 1D. Rename 3 test directories | ✅ | `UT1.31_ConfigDelegation`, `UT1.34_ConfigDelegationVerification`, `ST1.8_ConfigDelegationE2E` all present |

Config delegation verification command returns clean:
```bash
grep -rn "os.environ\|import hvac\|overlay_secrets\|from cloud_dog_vdb.secrets" cloud_dog_vdb/ --include="*.py" | grep -v __pycache__
# → zero results
```

---

### Issue 2 — SA1 Module Alignment ✅ MOSTLY RESOLVED

**Resolved items:**
- `secrets/` directory deleted ✅
- `registry/` duplicate directory removed ✅
- Duplicate converter files (`deepdoc.py`, `mineru.py`, `pandas.py`) removed — only `*_conv.py` versions remain ✅

**Remaining extra files (non-blocking — additive, not violations):**

| File/Directory | Purpose | Recommendation |
|----------------|---------|----------------|
| `factory.py` (top-level) | `get_vdb_client()` factory imported by `__init__.py` | Add to SA1 or fold into `runtime/factory.py` |
| `embeddings/` (3 files) | Standalone embedding provider helpers | Add to SA1 (supplements `ingestion/embed.py`) |
| `adapters/vector_utils.py` | Deterministic vector generation for testing | Move to `testing/` or add to SA1 |
| `runtime/` (3 files) | `VDBClient` and factory | Add to SA1 |

**Recommendation:** Update ARCHITECTURE.md SA1 to include these files. This is documentation-only — the package is functionally complete.

---

### Issue 3 — Compatibility Normaliser (FR1.34) ✅ DELIVERED

- `cloud_dog_vdb/compat/response_normaliser.py` — **178 lines**
- Per-backend mappings for Chroma, Qdrant, Weaviate, OpenSearch, PGVector
- Normalises backend responses to unified `SearchResult` / `Record` models
- Test directory `UT1.35_ResponseNormaliser` present

---

### Issue 4 — Client-Only Integration Mode (FR1.35) ✅ DELIVERED

- `cloud_dog_vdb/remote/client.py` — **86 lines**
- `RemoteVDBClient` proxy delegating all ops to remote VDB via HTTP
- No local backend dependency required
- Test directories `UT1.36_RemoteProxy` and `AT1.5_ClientOnlyIntegration` present

---

### Issue 5 — Collection Schema Versioning (FR1.36) ✅ DELIVERED

- `cloud_dog_vdb/versioning/schema_version.py` — **129 lines**
- Tracks dimension count, metadata fields, embedding model, version number per collection
- Version mismatch detection at query time
- Migration utility for detecting dimension changes
- Test directory `UT1.37_SchemaVersioning` present

---

### Issue 6 — Test Directory Alignment ✅ RESOLVED

- Old scaffold IT directories (ChromaRemoteCollection, ChromaLocalCollection, QdrantCollection, WeaviateCollection, OpenSearchIndex, PGVectorTable, EmbeddingProviders) removed
- All IT directories match TESTS.md v0.2.0 names (IT1.1_ChromaCRUD through IT1.13_IngestionPipelineEndToEnd)
- Config delegation test directories renamed per v0.2.0 spec

---

## Verification — Full Suite

```bash
set -a; source /opt/iac/Development/cloud-dog-ai/env-vault-admin; set +a

# 1. Config delegation check — MUST return zero hits
grep -rn "os.environ\|import hvac\|overlay_secrets\|from cloud_dog_vdb.secrets" cloud_dog_vdb/ --include="*.py" | grep -v __pycache__

# 2. secrets/ directory MUST NOT exist
test ! -d cloud_dog_vdb/secrets && echo "PASS" || echo "FAIL"

# 3. All tests pass
.venv/bin/pytest tests --env tests/env-UT --env tests/env-ST --env tests/env-IT --env tests/env-AT -q

# 4. Lint clean
.venv/bin/ruff check cloud_dog_vdb tests
.venv/bin/ruff format --check cloud_dog_vdb tests

# 5. Build
.venv/bin/python -m build --no-isolation
```

## pyproject.toml version

```toml
version = "0.2.0"
```

---

## MANDATORY COMPLETION REPORT

When finished, write your report to:
**`/opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards/packages/backend/platform-vdb/working/W28A-116-FIX-VDB-REPORT.md`**

Your report MUST include ALL of the following:

### 1. Run summary
- List every file changed and what was changed
- List every test fixed and how

### 2. Test results (REAL counts from actual runs)
```
QT: Xp / Yf
UT: Xp / Yf
ST: Xp / Yf
IT: Xp / Yf
AT: Xp / Yf
Ruff: X issues
```

### 3. Verdict
State one of: **PASS** (100% green) / **PARTIAL** (some fixed, some remain) / **FAIL** (no improvement) / **BLOCKED** (cannot proceed)

If not PASS, list every remaining failure with classification: `CODE_BUG`, `ENV_CONFIG`, `INFRA_MISSING`, `EXT_SERVICE`

### 4. Evidence logs
All logs MUST be saved to `working/` directory:
```
working/w28a-116-qt.log
working/w28a-116-ut.log
working/w28a-116-st.log
working/w28a-116-it.log
working/w28a-116-at.log
working/w28a-116-ruff.log
```

### 5. RULES.md COMPLIANCE WARRANTY

Copy this EXACTLY into your report:
```
I warrant that:
1. I have read RULES.md IN FULL before starting work
2. ALL code I produced is 100% compliant with RULES.md
3. ALL test results reported are REAL — exact counts from actual runs
4. I have NOT weakened any test
5. I have NOT stored, copied, or exposed any credentials
6. ALL credentials come from Vault or git-ignored env files
7. I have NOT modified files outside this package
```
