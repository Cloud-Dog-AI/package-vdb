# platform-vdb — TESTS.md

**Package:** `cloud_dog_vdb`  
**Version:** `0.5.0`  
**Standards:** PS-60, PS-95  
**Status:** Active (parser/OCR/table/Infinity uplift implemented on 2026-02-28)

---

## Test Strategy

### Hierarchy

- **UT**: Pure unit behaviour, mocks/local helpers allowed.
- **ST**: System-level package behaviour with real parser/provider endpoints.
- **IT**: Real backend integration (Chroma, Qdrant, Weaviate, OpenSearch, PGVector, Infinity where reachable).
- **AT**: Real application-level flows using real parser and backend services.
- **QT**: Security validation (real backend where applicable).

### Rules Enforced

- `--env` is mandatory for all test runs.
- Env precedence is honoured via test fixture loading.
- **IT/AT/QT preconditions hard-fail** when Vault prerequisites are missing/unreachable.
- `pytest.skip()` is not used for IT/AT/QT preconditions.
- Backend write-path probes run before real backend suites (`chroma_ready`, `qdrant_ready`, `weaviate_ready`, `opensearch_ready`, `pgvector_ready`).
- Skip counts are always reported with pass/fail counts.

---

## Environment Files

- `tests/env-UT`
  - `TEST_ENV_TIER=UT`
- `tests/env-ST`
  - `TEST_ENV_TIER=ST`
  - `MINERU_BASE_URL=https://mineruapi.cloud-dog.net`
  - `MINERU_TIMEOUT_SECONDS=180`
  - `MINERU_REQUEST_RETRIES=5`
  - `MARKER_MCP_BASE_URL=https://marker0.cloud-dog.net`
- `tests/env-IT`
  - `TEST_ENV_TIER=IT`
  - `VAULT_ADDR=https://vault0.cloud-dog.net`
  - `VAULT_MOUNT_POINT=cloud_dog_ai`
  - `VAULT_CONFIG_PATH=config`
  - `MINERU_BASE_URL=https://mineruapi.cloud-dog.net`
  - `MINERU_TIMEOUT_SECONDS=180`
  - `MINERU_REQUEST_RETRIES=5`
  - `MARKER_MCP_BASE_URL=https://marker0.cloud-dog.net`
  - Local parser commands enabled: `DEEPDOC_COMMAND`, `DOCLING_COMMAND`, `TRANSFORMERS_COMMAND`
- `tests/env-AT`
  - `TEST_ENV_TIER=AT`
  - `VAULT_ADDR=https://vault0.cloud-dog.net`
  - `VAULT_MOUNT_POINT=cloud_dog_ai`
  - `VAULT_CONFIG_PATH=config`
  - `MINERU_BASE_URL=https://mineruapi.cloud-dog.net`
  - `MINERU_TIMEOUT_SECONDS=180`
  - `MINERU_REQUEST_RETRIES=5`
  - `MARKER_MCP_BASE_URL=https://marker0.cloud-dog.net`
  - Local parser commands enabled: `DEEPDOC_COMMAND`, `DOCLING_COMMAND`, `TRANSFORMERS_COMMAND`
- `tests/env-PT`
  - `TEST_ENV_TIER=PT`
  - `MINERU_TIMEOUT_SECONDS=300`
  - `MINERU_REQUEST_RETRIES=5`
  - `MINERU_PARSE_BACKEND=pipeline`
  - `MINERU_PARSE_METHOD=auto`
  - `MINERU_FORMULA_ENABLE=false`, `MINERU_TABLE_ENABLE=false`
  - `MINERU_RETURN_MIDDLE_JSON=false`, `MINERU_RETURN_IMAGES=false`
  - Local parser commands enabled: `DEEPDOC_COMMAND`, `DOCLING_COMMAND`, `TRANSFORMERS_COMMAND`
- `tests/env-PT-PERF`
  - `TEST_ENV_TIER=PT`
  - `MINERU_TIMEOUT_SECONDS=300`
  - `MINERU_REQUEST_RETRIES=5`
  - `MINERU_PARSE_BACKEND=pipeline`
  - `MINERU_PARSE_METHOD=auto`
  - `MINERU_FORMULA_ENABLE=false`, `MINERU_TABLE_ENABLE=false`
  - `MINERU_RETURN_MIDDLE_JSON=false`, `MINERU_RETURN_IMAGES=false`
  - Local parser commands enabled: `DEEPDOC_COMMAND`, `DOCLING_COMMAND`, `TRANSFORMERS_COMMAND`
- `tests/env-CORPUS-SMALL`, `tests/env-CORPUS-MEDIUM`, `tests/env-CORPUS-LARGE`
  - Scenario corpus slicing via `CORPUS_INCLUDE_IDS` for staged small/medium/large validation.
- `tests/env-REQUIRE-ALL-PARSERS`
  - Enforces strict provider enablement gate (`REQUIRE_ALL_PDF_PARSERS=true`).

`VAULT_TOKEN` is intentionally not stored in repository env files and must come from sourced Vault environment.

---

## Agent Execution Guidance

- Follow [AGENTS.md](AGENTS.md) for strict run order and evidence updates.
- For parser tiers, execute staged corpus slices (`SMALL` -> `MEDIUM` -> `LARGE`) before full-corpus runs.
- Do not treat parser/provider gates as complete until both:
  - parser correctness (`tests/parser`) and
  - parser performance (`tests/parser_performance`)
  pass at the intended corpus size.

---

## Directory Structure (Current)

```text
tests/
  conftest.py
  env-UT
  env-ST
  env-IT
  env-AT
  env-PT
  env-PT-PERF
  env-CT
  unit/
    UT1.1 ... UT1.43
    UT2.1 ... UT2.15
  system/
    ST1.1 ... ST1.12
    ST2.1 ... ST2.8
  integration/
    IT1.1 ... IT1.13
    IT2.1 ... IT2.11
  application/
    AT1.1 ... AT1.5
    AT2.1 ... AT2.2
  security/
    QT1.1 ... QT1.4
    QT2.1 ... QT2.7
  parser/
    PT1.1 ... PT1.8
  parser_performance/
    PT2.1 ... PT2.6
  compatibility/
    CT1.1 ... CT1.7
```

### Reclassifications Applied (2026-02-20)

- `IT1.11` old local test moved to `ST1.9`; new `IT1.11` now real Chroma + real Qdrant.
- `IT1.13` old pure lifecycle function test moved to `UT1.38`; new `IT1.13` now real Chroma lifecycle state validation.
- `AT1.1` → `ST1.10`, `AT1.2` → `ST1.11`, `AT1.3` → `ST1.12` (old local-mode tests).
- `AT1.4` → `UT1.39` (old mock conformance test), with new real-adapter `AT1.4` replacement.
- `AT1.5` → `UT1.40` (old mock transport test), with new endpoint-backed `AT1.5` replacement.
- `QT1.1` → `UT1.41`, `QT1.2` → `UT1.42`, `QT1.4` → `UT1.43` (old pure logic tests), with new real-backend QT replacements.

---

## Test Inventory Summary

- **UT folders:** 58
- **ST folders:** 20
- **IT folders:** 25
- **AT folders:** 7
- **QT folders:** 11
- **PT folders:** 15
- **CT folders:** 7

---

## Run History

| Date (UTC) | Scope | Command | Passed | Failed | Skipped | Notes |
|---|---|---|---:|---:|---:|---|
| 2026-03-11 | ST3 diagnostic (slice mismatch evidence) | `set -o pipefail; timeout 7200 .venv/bin/pytest tests/system/ST3.* --env tests/env-ST --env tests/env-CORPUS-SMALL -vv -s --durations=0 \| tee working/fix-vdb-tests-st3-rerun.log` | 3 | 1 | 0 | Failure showed corpus-slice mismatch (`ST3.4` selected fallback file and hit size guard). Prompted strict corpus ID enforcement fix to avoid hidden fallback behaviour. |
| 2026-03-11 | ST3 rerun (post-fix, full manifest) | `set -o pipefail; set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 7200 .venv/bin/pytest tests/system/ST3.1_MarkerMcpSyncParse tests/system/ST3.2_MarkerMcpAsyncParse tests/system/ST3.3_MarkerMcpImageArtefacts tests/system/ST3.4_MarkerMcpLargeDocument --env tests/env-ST -vv -s --durations=0 \| tee working/fix-vdb-tests-st3-rerun-envst.log` | 4 | 0 | 0 | Long-call timings captured: ST3.1 `238.88s`, ST3.2 `232.17s`, ST3.3 `19.18s`, ST3.4 `18.36s`; confirms prior "hang" was long OCR runtime, not deadlock. |
| 2026-03-11 | Full UT+ST gate (post-fix) | `set -o pipefail; timeout 10800 .venv/bin/pytest tests/unit tests/system --env tests/env-UT --env tests/env-ST -v --durations=0 \| tee working/fix-vdb-tests-ut-st-rerun.log` | 102 | 0 | 0 | Clean gate with ST3 included; no skips. Slowest tests: ST3.1 `240.11s`, ST3.2 `229.07s`. |
| 2026-03-11 | Full IT+AT gate (post-fix) | `set -o pipefail; set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 5400 .venv/bin/pytest tests/integration tests/application --env tests/env-IT --env tests/env-AT -v --durations=0 \| tee working/fix-vdb-tests-it-at-rerun.log` | 31 | 0 | 0 | Clean gate with real backends/services; no skips. |
| 2026-03-11 | Full QT gate (post-fix) | `set -o pipefail; set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 1800 .venv/bin/pytest tests/security --env tests/env-IT -v --durations=0 \| tee working/fix-vdb-tests-qt-rerun.log` | 12 | 0 | 0 | Clean security gate; no skips. |
| 2026-03-08 | W25A-75 marker MCP transport validation | `ruff check`; `ruff format --check`; `pytest tests/unit --env tests/env-UT`; `pytest tests/system/...` (ST split + ST3 reruns); `pytest tests/compatibility --env tests/env-CT`; `pytest tests/integration --env tests/env-IT` (tail rerun for IT2.7/2.8/2.9); `pytest tests/application --env tests/env-AT`; `pytest tests/security --env tests/env-IT`; `python -m build`; install+smoke `0.5.0` | 155 | 0 | 0 | Marker MCP `/mcp` initialize+tools/call probe confirmed streamable transport; transient live-service instability observed (`OCR worker busy`) on first ST3.1 attempt and cleared on rerun. Full evidence in `working/W25A-75-MARKER-MCP-VALIDATE-REPORT.md` and `tmp/w25a_75_*.log`. |
| 2026-03-01 | 0.4.1 packaging/publish | `.venv/bin/python -m build`; `.venv/bin/pip install --force-reinstall dist/cloud_dog_vdb-0.4.1-py3-none-any.whl`; `.venv/bin/python -c "import cloud_dog_vdb; print(cloud_dog_vdb.__version__)"`; `.venv/bin/twine upload dist/cloud_dog_vdb-0.4.1-py3-none-any.whl dist/cloud_dog_vdb-0.4.1.tar.gz` | 4 | 0 | 0 | Build/install/smoke and private PyPI publish succeeded for `0.4.1`. |
| 2026-03-01 | W13A strict baseline (failure evidence) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT --env tests/env-REQUIRE-ALL-PARSERS -q \| tee /tmp/w13a_it211.log` | 0 | 1 | 0 | First failure: `AssertionError: expected parser_provider=mineru, actual=internal; mineru direct parse succeeded: text_chars=672, provider_version=api-0.1.0`. |
| 2026-03-01 | W13A strict baseline (failure evidence) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-REQUIRE-ALL-PARSERS -q \| tee /tmp/w13a_at22.log` | 0 | 1 | 0 | First failure: `AssertionError: expected parser_provider=mineru, actual=internal; mineru direct parse succeeded: text_chars=672, provider_version=api-0.1.0`. |
| 2026-03-01 | W13A local wrapper gate | `.venv/bin/pytest tests/parser/PT1.2_DeepdocAdapter tests/parser/PT1.3_DoclingAdapter tests/parser/PT1.8_TransformersAdapter --env tests/env-PT --env tests/env-CORPUS-SMALL -q \| tee /tmp/w13a_pt_local_small.log` | 3 | 0 | 0 | Pass after local wrapper enrichment and import-path correction. |
| 2026-03-01 | W13A strict recheck (post-fix) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT --env tests/env-REQUIRE-ALL-PARSERS -q \| tee /tmp/w13a_it211.log` | 1 | 0 | 0 | Pass after source-uri filename/MIME inference fix in ingestion pipeline. |
| 2026-03-01 | W13A strict recheck (post-fix) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-REQUIRE-ALL-PARSERS -q \| tee /tmp/w13a_at22.log` | 1 | 0 | 0 | Pass after source-uri filename/MIME inference fix in ingestion pipeline. |
| 2026-03-01 | W13A gate 3 | `.venv/bin/pytest tests/unit tests/system --env tests/env-UT --env tests/env-ST -q \| tee /tmp/w13a_ut_st.log` | 88 | 0 | 1 | Includes `UT2.15` regression test for bytes+source_uri filename/MIME inference. |
| 2026-03-01 | W13A gate 4 | `.venv/bin/pytest tests/compatibility --env tests/env-CT -q \| tee /tmp/w13a_ct.log` | 7 | 0 | 0 | Compatibility suite pass. |
| 2026-03-01 | W13A gate 5 | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration --env tests/env-IT -q \| tee /tmp/w13a_it.log` | 24 | 0 | 0 | Full integration matrix pass. |
| 2026-03-01 | W13A gate 6 (failure evidence) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 1800 .venv/bin/pytest tests/parser --env tests/env-PT --env tests/env-CORPUS-LARGE -q \| tee /tmp/w13a_pt1_large.log` | 5 | 1 | 2 | First failure: `assert float(summary["success_ratio"]) >= 0.90`, observed `0.75` in `PT1.1` due MinerU CUDA OOM on `IBRD-Financial-Statements-June-2025.pdf`. |
| 2026-03-01 | W13A gate 6 (failure evidence) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 1800 .venv/bin/pytest tests/parser --env tests/env-PT --env tests/env-CORPUS-LARGE -q \| tee /tmp/w13a_pt1_large.log` | 5 | 1 | 2 | First failure: `RuntimeError: mineru health_check returned false` in `tests/parser/_provider_matrix.py:204` (transient liveness probe instability). |
| 2026-03-01 | W13A gate 6 (final) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 1800 .venv/bin/pytest tests/parser --env tests/env-PT --env tests/env-CORPUS-LARGE -q \| tee /tmp/w13a_pt1_large.log` | 6 | 0 | 2 | Pass after low-VRAM-first MinerU parse options and resilient MinerU health probing. |
| 2026-03-01 | W13A gate 7 (final) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 1800 .venv/bin/pytest tests/parser_performance --env tests/env-PT-PERF --env tests/env-CORPUS-LARGE -q \| tee /tmp/w13a_pt2_large.log` | 6 | 0 | 0 | Parser performance gate pass on large corpus. |
| 2026-03-01 | W13A gate 8 | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application --env tests/env-AT -q \| tee /tmp/w13a_at.log` | 7 | 0 | 0 | Application gate pass. |
| 2026-03-01 | W13A gate 9 | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/security --env tests/env-IT -q \| tee /tmp/w13a_qt.log` | 12 | 0 | 0 | Security gate pass. |
| 2026-03-01 | W13A gates 10-12 | `.venv/bin/python -m build \| tee /tmp/w13a_build.log`; `.venv/bin/pip install --force-reinstall dist/cloud_dog_vdb-0.4.0-py3-none-any.whl \| tee /tmp/w13a_install.log`; `.venv/bin/python -c "import cloud_dog_vdb; print(cloud_dog_vdb.__version__)" \| tee /tmp/w13a_smoke.log` | 3 | 0 | 0 | Build/install/smoke all pass (`0.4.0`). |
| 2026-03-01 | Local parser dependency install | `.venv/bin/pip install transformers==4.57.1 docling-parse==5.4.0` | 1 | 0 | 0 | Installed local parser dependencies used by command-mode wrappers. |
| 2026-03-01 | Parser provider config propagation fix | `.venv/bin/pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT --env tests/env-REQUIRE-ALL-PARSERS -q` | 0 | 1 | 0 | Initial failure: deepdoc/docling/transformers resolved but helper omitted `enabled` in `parser_services` payload; parser fallback to `internal`. |
| 2026-03-01 | All parser enforcement IT2.11 | `... pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT --env tests/env-REQUIRE-ALL-PARSERS -q` | 1 | 0 | 0 | Pass after enabling local command parsers and including `enabled` in parser service payload. |
| 2026-03-01 | All parser enforcement AT2.2 | `... pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-REQUIRE-ALL-PARSERS -q` | 1 | 0 | 0 | Pass with local command parsers (`deepdoc`, `docling`, `transformers`) enabled. |
| 2026-03-01 | Gate 6 (staged large) | `timeout 1800 ... pytest tests/parser --env tests/env-PT --env tests/env-CORPUS-LARGE -q` | 6 | 0 | 2 | PT1 large slice completed and passed with local parser command adapters enabled. |
| 2026-03-01 | PT2.4 large (diagnostic) | `timeout 1800 ... pytest tests/parser_performance/PT2.4_QualityInvariantPassRate --env tests/env-PT-PERF --env tests/env-CORPUS-LARGE -q` | 0 | 1 | 0 | Initial fail: `docling quality_invariant_pass_rate=0.75 < 0.95` (heading invariant miss on IBRD file). |
| 2026-03-01 | PT2.4 large (rerun) | `timeout 1800 ... pytest tests/parser_performance/PT2.4_QualityInvariantPassRate --env tests/env-PT-PERF --env tests/env-CORPUS-LARGE -q` | 1 | 0 | 0 | Pass after wrapper heading-structure enrichment via PDF outline headings. |
| 2026-03-01 | Gate 7 (staged large) | `timeout 1800 ... pytest tests/parser_performance --env tests/env-PT-PERF --env tests/env-CORPUS-LARGE -q` | 6 | 0 | 0 | PT2 large slice completed and passed (`~15m56s`). |
| 2026-03-01 | MinerU API probe (repeat) | `curl ... /file_parse` (small PDF `ITEM_COD-0012-0001-_-089.pdf`, `pipeline/auto`) | 6 | 0 | 0 | Full/page-0 calls all `HTTP 200`; observed variable latency window (~5s to ~30s). |
| 2026-03-01 | Gate 1 | `.venv/bin/ruff check` | 1 | 0 | 0 | Lint pass. |
| 2026-03-01 | Gate 2 | `.venv/bin/ruff format --check` | 1 | 0 | 0 | Initial failure on 3 files; formatted and re-checked clean. |
| 2026-03-01 | Gate 3 | `.venv/bin/pytest tests/unit tests/system --env tests/env-UT --env tests/env-ST -q` | 87 | 0 | 1 | UT/ST gate pass. |
| 2026-03-01 | Gate 4 | `.venv/bin/pytest tests/compatibility --env tests/env-CT -q` | 7 | 0 | 0 | CT1 compatibility gate pass. |
| 2026-03-01 | Gate 5 | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration --env tests/env-IT -q` | 24 | 0 | 0 | Full integration matrix pass (includes Infinity profile where configured). |
| 2026-03-01 | All parser enforcement IT2.11 (recheck) | `... pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT --env tests/env-REQUIRE-ALL-PARSERS -q` | 0 | 1 | 0 | Failed by design: `REQUIRE_ALL_PDF_PARSERS=true` with disabled `deepdoc`, `docling`, `transformers`. |
| 2026-03-01 | All parser enforcement AT2.2 (recheck) | `... pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-REQUIRE-ALL-PARSERS -q` | 0 | 1 | 0 | Failed by design: `REQUIRE_ALL_PDF_PARSERS=true` with disabled `deepdoc`, `docling`, `transformers`. |
| 2026-03-01 | Gate 6 (staged small) | `... pytest tests/parser --env tests/env-PT --env tests/env-CORPUS-SMALL -q` | 3 | 0 | 5 | PT1 slice-aware fix applied (`PT1.5` now skips when selected slice has no OCR-required doc). |
| 2026-03-01 | Gate 7 (staged small) | `... pytest tests/parser_performance --env tests/env-PT-PERF --env tests/env-CORPUS-SMALL -q` | 6 | 0 | 0 | PT2 small slice pass. |
| 2026-03-01 | Gate 6 (staged medium) | `... pytest tests/parser --env tests/env-PT --env tests/env-CORPUS-MEDIUM -q` | 4 | 0 | 4 | PT1 medium slice pass. |
| 2026-03-01 | Gate 7 (staged medium) | `... pytest tests/parser_performance --env tests/env-PT-PERF --env tests/env-CORPUS-MEDIUM -q` | 6 | 0 | 0 | PT2 medium slice pass. |
| 2026-03-01 | Gate 6 (staged large, timed) | `timeout 1800 ... pytest tests/parser --env tests/env-PT --env tests/env-CORPUS-LARGE -q` | 0 | 0 | 0 | Terminated after extended external wait windows; no conclusive result captured in this run. |
| 2026-03-01 | Gate 8 | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application --env tests/env-AT -q` | 7 | 0 | 0 | Full application tier pass. |
| 2026-03-01 | Gate 9 | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/security --env tests/env-IT -q` | 12 | 0 | 0 | Full security tier pass. |
| 2026-03-01 | Gates 10-12 | `.venv/bin/python -m build`, `.venv/bin/pip install --force-reinstall dist/cloud_dog_vdb-0.4.0-py3-none-any.whl`, smoke import script | 3 | 0 | 0 | Build/install/smoke pass; verified `cloud_dog_vdb.__version__` and `InfinityAdapter` factory construction. |
| 2026-03-01 | ST2 full baseline | `.venv/bin/pytest tests/system/ST2.* --env tests/env-ST -q` | 7 | 0 | 1 | Baseline pass on long-timeout MinerU profile; Marker-MCP hold skip unchanged. |
| 2026-03-01 | IT2 full baseline | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration/IT2.* --env tests/env-IT -q` | 11 | 0 | 0 | Baseline pass after long-timeout/retry uplift. |
| 2026-03-01 | AT2 full baseline | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application/AT2.* --env tests/env-AT -q` | 2 | 0 | 0 | Baseline pass after long-timeout/retry uplift. |
| 2026-03-01 | UT2.12 baseline | `.venv/bin/pytest tests/unit/UT2.12_ProviderHealthContract --env tests/env-UT -q` | 2 | 0 | 0 | MinerU adaptive retry/page-fallback contracts pass in current state. |
| 2026-03-01 | MinerU API probe | `curl ... /file_parse` (small PDF `ITEM_COD-0012-0001-_-089.pdf`) | 6 | 0 | 0 | Full parse: 3/3 success, ~3.02–3.36s; page-0 parse: 3/3 success, ~1.97–2.34s. |
| 2026-03-01 | ST2 small corpus | `.venv/bin/pytest tests/system/ST2.* --env tests/env-ST --env tests/env-CORPUS-SMALL -q` | 7 | 0 | 1 | Small-slice parser path stable; Marker-MCP intentionally skipped. |
| 2026-03-01 | IT2.11 small corpus | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT --env tests/env-CORPUS-SMALL -q` | 1 | 0 | 0 | Strict mineru provider assertion passed. |
| 2026-03-01 | AT2.2 small corpus | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-CORPUS-SMALL -q` | 1 | 0 | 0 | Strict mineru provider assertion passed. |
| 2026-03-01 | PT1.1 small corpus | `.venv/bin/pytest tests/parser/PT1.1_MineruAdapter --env tests/env-PT --env tests/env-CORPUS-SMALL -q` | 1 | 0 | 0 | Quality matrix passed after heading/table heuristic uplift (`#` and `<table>` recognition). |
| 2026-03-01 | ST2 medium corpus | `.venv/bin/pytest tests/system/ST2.* --env tests/env-ST --env tests/env-CORPUS-MEDIUM -q` | 7 | 0 | 1 | Medium-slice system suites passed. |
| 2026-03-01 | IT2.11 medium corpus | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT --env tests/env-CORPUS-MEDIUM -q` | 0 | 1 | 0 | Failed strict mineru assertion during upstream GPU OOM window (`actual=internal`). |
| 2026-03-01 | AT2.2 medium corpus | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-CORPUS-MEDIUM -q` | 0 | 1 | 0 | Failed strict mineru assertion during upstream GPU OOM window (`actual=internal`). |
| 2026-03-01 | PT1.1 medium corpus | `.venv/bin/pytest tests/parser/PT1.1_MineruAdapter --env tests/env-PT --env tests/env-CORPUS-MEDIUM -q` | 0 | 1 | 0 | `success_ratio=0.75` (<0.90) due upstream OOM/timeouts on medium set. |
| 2026-03-01 | IT2.11 large corpus | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT --env tests/env-CORPUS-LARGE -q` | 1 | 0 | 0 | Strict mineru provider assertion passed on large-slice scenario. |
| 2026-03-01 | AT2.2 large corpus | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-CORPUS-LARGE -q` | 1 | 0 | 0 | Strict mineru provider assertion passed on large-slice scenario. |
| 2026-03-01 | PT1.1 large corpus timed | `timeout 240 .venv/bin/pytest tests/parser/PT1.1_MineruAdapter --env tests/env-PT --env tests/env-CORPUS-LARGE -q` | 0 | 0 | 0 | Timed out (`EXIT_CODE=124`) before run completion. |
| 2026-03-01 | PT2 large corpus timed | `timeout 240 .venv/bin/pytest tests/parser_performance --env tests/env-PT-PERF --env tests/env-CORPUS-LARGE -q` | 0 | 0 | 0 | Timed out (`EXIT_CODE=124`) before run completion. |
| 2026-03-01 | All parser enforcement IT2.11 | `... --env tests/env-IT --env tests/env-REQUIRE-ALL-PARSERS ...` | 0 | 1 | 0 | Failed by design: disabled providers `deepdoc`, `docling`, `transformers`. |
| 2026-03-01 | All parser enforcement AT2.2 | `... --env tests/env-AT --env tests/env-REQUIRE-ALL-PARSERS ...` | 0 | 1 | 0 | Failed by design: disabled providers `deepdoc`, `docling`, `transformers`. |
| 2026-02-28 | ST2 targeted | `.venv/bin/pytest tests/system/ST2.1_LegacyPathParity tests/system/ST2.2_IRPathLocal --env tests/env-ST -q` | 2 | 0 | 0 | MinerU parser path tests pass using `pipeline/ocr` with single-page probe options and retry guard. |
| 2026-02-28 | ST2 full | `.venv/bin/pytest tests/system/ST2.* --env tests/env-ST -q` | 7 | 0 | 1 | Marker-MCP hold remains intentionally skipped (`ST2.8`). |
| 2026-02-28 | IT2 full | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration/IT2.* --env tests/env-IT -q` | 11 | 0 | 0 | Full IT2 parser-first/backend matrix pass with warning-free output. |
| 2026-02-28 | AT2 full | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application/AT2.* --env tests/env-AT -q` | 2 | 0 | 0 | Full AT2 suite pass. |
| 2026-02-28 | PT1.1 timed run | `timeout 180 .venv/bin/pytest tests/parser/PT1.1_MineruAdapter --env tests/env-PT -q` | 0 | 0 | 0 | Command hit timeout (`EXIT_CODE=124`) before completion under live MinerU latency; full corpus PT run not yet complete. |
| 2026-02-28 | PT2.4 timed run | `timeout 180 .venv/bin/pytest tests/parser_performance/PT2.4_QualityInvariantPassRate --env tests/env-PT-PERF -q` | 0 | 0 | 0 | Command hit timeout (`EXIT_CODE=124`) before completion under live MinerU latency; quality invariant gate remains open. |
| 2026-02-28 | UT2.12 | `.venv/bin/pytest tests/unit/UT2.12_ProviderHealthContract --env tests/env-UT -q` | 2 | 0 | 0 | MinerU adaptive retry + page fallback unit contracts pass. |
| 2026-02-28 | IT2.11 | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT -q` | 1 | 0 | 0 | Strict parser-provider matrix passes with MinerU `pipeline/ocr` options. |
| 2026-02-28 | AT2.2 | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT -q` | 1 | 0 | 0 | Strict parser-provider matrix passes with MinerU `pipeline/ocr` options. |
| 2026-02-28 | Parser (PT1) | `.venv/bin/pytest tests/parser --env tests/env-PT -q` | 3 | 2 | 3 | `mineru` and `marker_mcp` failed quality matrix with 0% success due upstream CUDA OOM; deepdoc/docling/transformers disabled by env and explicitly skipped. |
| 2026-02-28 | Parser Perf (PT2) | `.venv/bin/pytest tests/parser_performance --env tests/env-PT-PERF -q` | 4 | 2 | 0 | Throughput and quality floor gates failed because upstream parser endpoints returned OOM/timeouts across corpus. |
| 2026-02-28 | IT (full) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration --env tests/env-IT -q` | 23 | 1 | 0 | `IT2.11` strict provider matrix failed: expected `parser_provider=mineru`, actual `internal` (fallback engaged due parser failure). |
| 2026-02-28 | AT (full) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application --env tests/env-AT -q` | 6 | 1 | 0 | `AT2.2` strict provider matrix failed with same fallback condition (`mineru -> internal`). |
| 2026-02-28 | ST (full) | `.venv/bin/pytest tests/system --env tests/env-ST -q` | 18 | 2 | 0 | `ST2.1` and `ST2.2` failed on MinerU 500 CUDA OOM responses. |
| 2026-02-28 | ST (full) | `.venv/bin/pytest tests/system --env tests/env-ST -q` | 20 | 0 | 0 | Includes ST2 parser uplift suites against live parser endpoints. |
| 2026-02-28 | IT (full) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration --env tests/env-IT -q` | 22 | 1 | 0 | `IT2.6` failed: Infinity endpoint unreachable (`ConnectError`). |
| 2026-02-28 | IT2 parser-first | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration/IT2.* --env tests/env-IT -q` | 9 | 1 | 0 | Infinity backend unreachable; all other IT2 backends passed. |
| 2026-02-28 | AT (full) | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application --env tests/env-AT -q` | 6 | 0 | 0 | Includes parser-first AT2.1 flow. |
| 2026-02-28 | AT2 parser-first | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application/AT2.1_ParserFirstEndToEnd --env tests/env-AT -q` | 1 | 0 | 0 | Real MinerU + Chroma end-to-end flow. |
| 2026-02-20 | Full matrix | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests --env tests/env-UT --env tests/env-ST --env tests/env-IT --env tests/env-AT -q` | 86 | 0 | 0 | Full package pass across UT/ST/IT/AT/QT. |
| 2026-02-20 | UT + ST | `.venv/bin/pytest tests/unit tests/system --env tests/env-UT --env tests/env-ST -v` | 63 | 0 | 0 | Clean run; reclassified UT/ST suites included. |
| 2026-02-20 | IT + AT | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration tests/application --env tests/env-IT --env tests/env-AT -q` | 18 | 0 | 0 | Full pass. AT1.5 now uses a real HTTP proxy endpoint backed by real Chroma backend (no mock transport, no silent skip). |
| 2026-02-20 | QT | `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/security --env tests/env-IT -q` | 5 | 0 | 0 | Full QT suite passes against real backend preconditions. |

---

## Current Verification Status

- Completion gates currently met for:
  - Parser/OCR/table/module uplift implementation and backward-compat API checks.
  - New env files and endpoint wiring (`env-PT`, `env-PT-PERF`, `env-CT`, corpus-slice envs, long-timeout/retry profile).
  - ST2 full suite (`ST2.1`–`ST2.8`) with Marker-MCP intentionally disabled.
  - IT2 full suite (`IT2.1`–`IT2.11`) against real backends and live parser endpoint.
  - AT2 full suite (`AT2.1`–`AT2.2`) against live parser endpoint.
  - Small-corpus staged confidence path (ST2/IT2.11/AT2.2/PT1.1) against MinerU API.
  - Medium-corpus staged parser and parser-performance paths (`PT1`, `PT2`).
  - Large-corpus staged parser and parser-performance paths (`PT1`, `PT2`), including PT2 quality floor gate (`PT2.4`).
  - Strict all-parser enforcement gate (`REQUIRE_ALL_PDF_PARSERS=true`) now passing in both IT2.11 and AT2.2 with local command parser adapters.
  - Documentation stage assets now present (`AGENTS.md` + programme gate for evidence updates).
- Open gate:
  - ~~Marker-MCP testing is temporarily on hold and disabled in env tiers (`MARKER_MCP_ENABLED=false`) until service stabilisation.~~ **RESOLVED 2026-03-04:** Marker MCP service confirmed LIVE at `https://marker0.cloud-dog.net`. `MARKER_MCP_ENABLED=true` in all env tiers from 0.5.0.

---

## 0.4.0 Test Uplift (Implemented, Additive, Non-Breaking)

**Status:** Implemented  
**Date:** 2026-02-28  
**Reference:** `RELEASE_UPLIFT_PROPOSAL.md`

### New Coverage Objective

Add parser/OCR/table/chunk policy coverage while proving full backward compatibility with 0.3.x API/config/default behaviour.

### Proposed Additional Suite Types

- **PT**: Parser integration tests for external parser/OCR providers.
- **CT**: Compatibility/regression tests proving no breakage for existing consumers.

### Proposed New Suites

#### UT2 — Parser/OCR/IR Unit Suites
- `UT2.1_ParserCapabilities`
- `UT2.2_ParserPlannerSelection`
- `UT2.3_ParserFallbackPolicy`
- `UT2.4_DocumentIRSchema`
- `UT2.5_TableRenderPolicies`
- `UT2.6_TableJsonSchema`
- `UT2.7_OCRHeuristics`
- `UT2.8_OCRCostLimiter`
- `UT2.9_BoundaryAwareChunking`
- `UT2.10_ConfigAliasCompatibility`

#### ST2 — Local Pipeline End-to-End
- `ST2.1_LegacyPathParity`
- `ST2.2_IRPathLocal`
- `ST2.3_CheckpointResumeAcrossParse`
- `ST2.4_TableChunkFlow`

#### PT1 — External Parser/OCR Integrations
- `PT1.1_MineruAdapter`
- `PT1.2_DeepdocAdapter`
- `PT1.3_DoclingAdapter`
- `PT1.4_MarkerMcpAdapter`
- `PT1.5_OcrProviders`
- `PT1.8_TransformersAdapter`

#### PT2 — Parser/OCR Performance on Known Corpus
- `PT2.1_CorpusLatencyBenchmarks` (`p50`, `p95` parse latency)
- `PT2.2_CorpusThroughputBenchmarks` (pages/sec or bytes/sec)
- `PT2.3_OcrAutoDecisionRate` (OCR trigger ratio on scanned/mixed docs)
- `PT2.4_QualityInvariantPassRate` (minimum invariant pass threshold per doc class)

PT enforcement:
- enabled-but-unreachable provider MUST fail,
- explicitly disabled provider MAY skip with explicit reason.

#### IT2 — Backend Integration with Parser-First Pipeline
For each enabled backend profile:
- parser-first ingest -> search/filter retrieval -> lifecycle delete/purge checks.
- include Infinity backend profile (`dev.vdbs.infinity`) when enabled.

#### QT2 — External Provider Security
- `QT2.1_CommandAllowlist`
- `QT2.2_CommandTimeout`
- `QT2.3_EndpointAllowlist`
- `QT2.4_SecretRedaction`
- `QT2.5_PathTraversalGuard`

#### CT1 — Backward Compatibility Guarantees
- `CT1.1_PublicApiParity`
- `CT1.2_DefaultBehaviourParity`
- `CT1.3_MetadataIdentityParity`
- `CT1.4_ConfigCompatibility`
- `CT1.5_ErrorContractParity`

### Proposed Additional Env Files

- `tests/env-PT` for parser integrations.
- `tests/env-CT` for compatibility baselines.
- `tests/env-PT-PERF` for repeatable corpus performance runs.

`--env` remains mandatory for all runs.

### Corpus-Driven Test Inputs (`test-data/`)

Known files under `packages/backend/platform-vdb/test-data/` are used as deterministic parser/OCR fixtures, including:
- `Handwritten-Concern-Form-reporting-Domestic-Abuse-Good-Example.pdf`
- `SAMPLE OF RURAL COMPLETED FORM.pdf`
- `Z83-example.pdf`
- `IBRD-Financial-Statements-June-2025.pdf`
- `a10kfy2023filing.pdf`
- `Aon global-medical-trend-rates-report-2026.pdf`
- `NIST.SP.800-53r5.pdf`

Suite mapping MUST cover:
- handwritten/scanned OCR cases,
- table-heavy cases,
- mixed narrative + table cases,
- long-form regulatory/technical text cases.

### External Service Endpoint Sources

Parser integration tests MUST support endpoint config from:
- env files (`tests/env-PT`, `tests/env-PT-PERF`), and
- Vault-backed config via `cloud_dog_config`:
  - `dev.services.mineru`
  - `dev.services.marker_mcp` or `dev.services.markermcp`

Embedding validation in parser-first flows MUST run against configured embedding providers (env/Vault resolved).

### Proposed Release Gates (0.4.0)

1. Existing 0.3.x suites pass unchanged.
2. New UT2/ST2/IT2/PT1/PT2/QT2 suites pass where configured.
3. CT1 compatibility suite passes fully.
4. Security/redaction checks pass for external parser/OCR integrations.
5. Corpus performance report and quality-invariant summary are recorded per run.

### Programme References

- `PROGRAMME-0.4.0-DEVELOPMENT-BUILD-TEST.md` is the canonical development/build/test execution plan.
- `test-data/corpus-manifest.yaml` is the canonical parser/OCR corpus mapping and benchmark threshold definition.

### Implementation Status (2026-02-28)

Skeleton suites and env files have been created for:
- `UT2.1` through `UT2.12`
- `ST2.1` through `ST2.8`
- `IT2.1` through `IT2.10`
- `PT1.1` through `PT1.7`
- `PT2.1` through `PT2.6`
- `QT2.1` through `QT2.7`
- `CT1.1` through `CT1.7`
- `tests/env-PT`, `tests/env-PT-PERF`, `tests/env-CT`

---

## 0.5.0 Test Uplift — Marker MCP, Async Parse, Cross-Provider Comparison

**Status:** Implemented (live Marker gates depend on endpoint availability)  
**Date:** 2026-03-04  
**Reference:** REQUIREMENTS.md § 0.5.0

### Env File Changes (0.5.0)

- `tests/env-ST` — add `MARKER_MCP_ENABLED=true`, `MARKER_MCP_BASE_URL=https://marker0.cloud-dog.net`
- `tests/env-IT` — add `MARKER_MCP_ENABLED=true`
- `tests/env-AT` — add `MARKER_MCP_ENABLED=true`
- `tests/env-PT` — add `MARKER_MCP_ENABLED=true`
- `tests/env-PT-PERF` — add `MARKER_MCP_ENABLED=true`
- `tests/env-PT-COMPARE` — **NEW** env file for cross-provider comparison runs:
  - `TEST_ENV_TIER=PT`
  - `MARKER_MCP_ENABLED=true`
  - `MARKER_MCP_BASE_URL=https://marker0.cloud-dog.net`
  - `MINERU_BASE_URL=https://mineruapi.cloud-dog.net`
  - `MINERU_TIMEOUT_SECONDS=300`
  - `DEEPDOC_COMMAND=<path>` (if available)
  - `DOCLING_COMMAND=<path>` (if available)
  - `TRANSFORMERS_COMMAND=<path>` (if available)
  - `COMPARISON_OUTPUT_DIR=tests/comparison_reports/`
  - `COMPARISON_PROVIDERS=marker_mcp,mineru,deepdoc,docling,internal`

### New Test Suites (0.5.0)

#### UT3 — Async Parse + Marker Response Unit Tests

| ID | Name | Validates |
|----|------|-----------|
| UT3.1 | `UT3.1_MarkerResponseContract` | `_coerce_marker_text()` correctly reads `output` key from live Marker response envelope |
| UT3.2 | `UT3.2_MarkerImageExtraction` | Image dict decoded to artefact refs, image ref keys preserved |
| UT3.3 | `UT3.3_MarkerTOCExtraction` | `table_of_contents` metadata parsed into heading blocks |
| UT3.4 | `UT3.4_AsyncParseRunnerSubmitPoll` | Submit-poll-retrieve lifecycle with mock HTTP responses |
| UT3.5 | `UT3.5_AsyncParseRunnerTimeout` | Timeout handling raises `ParserTimeoutError` after max wait |
| UT3.6 | `UT3.6_AsyncParseRunnerCancellation` | Cancellation sends best-effort cancel request |
| UT3.7 | `UT3.7_AsyncModeAutoTrigger` | Async mode triggered automatically when doc size > threshold |
| UT3.8 | `UT3.8_SyncModeDefault` | Default mode remains synchronous (backwards compat) |

#### ST3 — Async Parse System Tests

| ID | Name | Validates |
|----|------|-----------|
| ST3.1 | `ST3.1_MarkerMcpSyncParse` | Sync parse of small PDF via live Marker endpoint |
| ST3.2 | `ST3.2_MarkerMcpAsyncParse` | Async parse (if supported) or long-timeout sync with heartbeat |
| ST3.3 | `ST3.3_MarkerMcpImageArtefacts` | Images from Marker response available in DocumentIR |
| ST3.4 | `ST3.4_MarkerMcpLargeDocument` | Large/complex corpus document parse completes within timeout |

#### PT3 — Cross-Provider Comparison Tests

| ID | Name | Validates |
|----|------|-----------|
| PT3.1 | `PT3.1_CorpusSmallComparison` | Run CORPUS-SMALL through all available providers, produce comparison JSON |
| PT3.2 | `PT3.2_CorpusMediumComparison` | Run CORPUS-MEDIUM through all available providers |
| PT3.3 | `PT3.3_CorpusLargeComparison` | Run CORPUS-LARGE through all available providers (long timeout) |
| PT3.4 | `PT3.4_MarkerVsMineruQuality` | Compare Marker vs MinerU quality metrics on full corpus |
| PT3.5 | `PT3.5_ProviderLatencyRanking` | Rank providers by p50/p95 latency per document category |
| PT3.6 | `PT3.6_TableExtractionComparison` | Compare table detection/extraction quality across providers on table-heavy PDFs |
| PT3.7 | `PT3.7_ImageExtractionComparison` | Compare image extraction across providers (Marker should excel here) |
| PT3.8 | `PT3.8_ComparisonReportGeneration` | Verify JSON + markdown report format and content |

#### PT1.4 Update — Marker MCP Adapter (Enabled)

- Remove `MARKER_MCP_ENABLED=false` gate.
- Set `MARKER_MCP_ENABLED=true` in all env tiers.
- Run against full corpus manifest (13 PDFs).
- Quality thresholds: `success_ratio >= 0.90`, `quality_invariant_pass_rate >= 0.70`.

### Updated Test Inventory Summary (0.5.0)

- **UT folders:** 66 (+8 from UT3)
- **ST folders:** 24 (+4 from ST3)
- **IT folders:** 25 (unchanged)
- **AT folders:** 7 (unchanged)
- **QT folders:** 11 (unchanged)
- **PT folders:** 23 (+8 from PT3)
- **CT folders:** 7 (unchanged)

### Proposed Release Gates (0.5.0)

1. All 0.4.x suites pass unchanged (backwards compat).
2. UT3 async parse unit tests pass.
3. ST3 Marker MCP system tests pass against live endpoint.
4. PT1.4 Marker MCP corpus quality gate passes (`success_ratio >= 0.90`).
5. PT3 cross-provider comparison runs produce valid reports.
6. PT3.4 Marker vs MinerU quality comparison recorded.
7. Build, install, smoke pass for 0.5.0.

### Execution Order (0.5.0)

```
1. Fix _coerce_marker_text() bug (UT3.1 must pass)
2. Add image/TOC extraction (UT3.2, UT3.3 must pass)
3. Add async_runner (UT3.4-UT3.8 must pass)
4. Wire Marker async mode (ST3.1-ST3.4 must pass)
5. Enable MARKER_MCP_ENABLED=true in env files
6. Run PT1.4 against full corpus
7. Add comparison framework (PT3.8 must pass)
8. Run PT3.1 (small), PT3.2 (medium), PT3.3 (large)
9. Record PT3.4 Marker vs MinerU results
10. Run full regression: UT/ST/IT/AT/QT/CT/PT
11. Build + publish 0.5.0
```
