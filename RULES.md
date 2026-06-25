# platform-vdb (cloud_dog_vdb) — Agent & Engineer Rules

**Version:** 2.0
**Date:** 2026-03-04
**Parent:** `cloud-dog-ai-platform-standards/RULES.md` v1.5

> **⛔ BINDING CONTRACT:** This document extends the platform-wide rules.
> Read the parent [Cloud-Dog AI Platform Common Rules](../../../RULES.md) **IN FULL** first.
> ALL platform rules apply without exception. This file adds package-specific rules ONLY.

---

## Section 1 — Platform Rules (Inherited)

All rules from `cloud-dog-ai-platform-standards/RULES.md` v1.5 apply without exception:
- **§ 1** Integrity and honesty (non-negotiable)
- **§ 2** Configuration precedence: `os.environ → env file → config.yaml → defaults.yaml`
- **§ 2.3** Credential management: Vault primary; `private/` only for credentials not yet in Vault
- **§ 2.4** Zero hardcoded values (zero tolerance)
- **§ 3** Server and process management (server_control.sh, Docker rules)
- **§ 4** Code and change management (approval rules, code standards, UK English)
- **§ 5** Testing rules (UT/ST/IT/AT hierarchy, real systems, forensic validation)
- **§ 6** Documentation standards (REQUIREMENTS, ARCHITECTURE, TESTS, TASKS, etc.)
- **§ 7** Repository structure
- **§ 8** Operational controls (timeouts, stop controls, verification)
- **§ 9** Security boundaries (project confinement, credential boundaries, network boundaries, scope discipline)
- **§ 10** Infrastructure protection (Vault config read-only, Terraform read-only)
- **§ 11** Vault path verification (never invent paths, query first)
- **§ 12** Implementation truthfulness (never claim done without evidence)
- **Mandatory Completion Warranty** required on every task completion

---

## Section 2 — Vault Configuration

### Load before any operation
```bash
set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a
```

### Vault sections used by this package
- `dev.vdbs` — Vector database connections (Chroma, Qdrant, OpenSearch, Weaviate, PGVector, Infinity)
- `dev.models` — Embedding model definitions (Ollama, OpenRouter, OpenAI-compat)
- `dev.services.mineru` — MinerU parser endpoint
- `dev.services.marker_mcp` — Marker MCP parser endpoint (when enabled)
- `dev.storage` — Object storage for uploaded binaries (optional S3)
- `dev.redis` — Redis/Valkey connection (optional job queue)
- `dev.repository` — PyPI registry credentials

---

## Section 3 — Package-Specific Rules

### 3.1 Backward Compatibility (Non-Negotiable)
- Public API parity — no breaking changes without version bump
- Default behaviour parity — existing consumers must not break
- Metadata identity parity — envelope shapes must not change
- Error/result envelope parity — error formats must not change

### 3.2 Public API Surface
- All public APIs MUST be importable from `cloud_dog_vdb/__init__.py`
- Optional dependencies MUST degrade gracefully when not installed
- Secret values MUST NEVER appear in logs, exceptions, or config dumps

### 3.3 Parser Providers
- Local parser command adapters supported without Vault:
  - `deepdoc` via `tests/tools/local_deepdoc_parser.py`
  - `docling` via `tests/tools/local_docling_parser.py`
  - `transformers` via `tests/tools/local_transformers_parser.py`
- MinerU/Marker parsers require Vault (`dev.services.mineru`, `dev.services.marker_mcp`)

### 3.4 VDB Backend Contract
- All VDB operations MUST go through the adapter layer
- NEVER import `chromadb`, `qdrant_client`, `opensearchpy`, `weaviate`, `pgvector` directly in consumer code
- Each backend MUST pass the same contract test suite
- Backend configuration comes from Vault `dev.vdbs` section

---

## Section 4 — Testing Rules (Package-Specific Extensions)

Platform testing rules (§ 5) apply in full. This section adds platform-vdb specifics.

### 4.1 Test Tiers
- `tests/unit/` + `tests/system/` — `--env tests/env-UT --env tests/env-ST`
- `tests/compatibility/` — `--env tests/env-CT`
- `tests/integration/` — `--env tests/env-IT` (requires Vault)
- `tests/application/` — `--env tests/env-AT` (requires Vault)
- `tests/parser/` — `--env tests/env-PT --env tests/env-CORPUS-{SMALL,MEDIUM,LARGE}`
- `tests/parser_performance/` — `--env tests/env-PT-PERF --env tests/env-CORPUS-{SMALL,MEDIUM,LARGE}`
- `tests/security/` — `--env tests/env-IT` (requires Vault)

### 4.2 Real Systems Only (ST/IT/AT/PT/QT)
- Use real dependencies — no stubs, silent fallbacks, or fake success paths
- If a real system is unavailable, the test MUST fail explicitly (`pytest.fail()`) — not skip or pass with fake data
- Required backend proof per VDB:
  - **Chroma**: HTTP check via `curl` against collection/object state
  - **Qdrant**: HTTP check via `curl` against collection point
  - **Weaviate**: HTTP check via `curl` against object endpoint
  - **OpenSearch**: HTTP check via `curl` against `_doc` endpoint
  - **PGVector**: SQL verification via `psql` (not HTTP)

### 4.3 Corpus Slicing (Recommended Progression)
1. `tests/parser` + `env-CORPUS-SMALL`
2. `tests/parser_performance` + `env-CORPUS-SMALL`
3. `tests/parser` + `env-CORPUS-MEDIUM`
4. `tests/parser_performance` + `env-CORPUS-MEDIUM`
5. Large slice under explicit timeout guard (`timeout 1800`)

### 4.4 Release Gate
Before claiming release readiness:
- All test tiers pass with real systems
- `ruff check` + `ruff format --check` clean
- `python -m build` succeeds
- Smoke import: `python -c "import cloud_dog_vdb; print(cloud_dog_vdb.__version__)"`
- Update TESTS.md, README.md, PROGRAMME doc, RELEASE_UPLIFT_PROPOSAL as needed

---

*Last updated: 2026-03-04*
