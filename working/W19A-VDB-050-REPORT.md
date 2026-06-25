# W19A VDB 0.5.0 Report

Date: 2026-03-04
Package: `cloud_dog_vdb`
Target: `0.5.0`

## Scope Delivered

- Marker MCP response contract fix (`output` key preferred)
- Marker image extraction -> `DocumentIR.artefact_refs`
- Marker TOC extraction -> heading `TextBlock`s
- Async parse runner added (`AsyncParseRunner`) with submit/poll/retrieve, timeout, cancel, sync fallback heartbeats
- Marker provider async wiring added (`async_mode`, auto-trigger threshold, async endpoint options)
- Marker provider resilience uplift: bounded request retries for transient route/transport errors
- Cross-provider comparison framework added:
  - `cloud_dog_vdb/testing/comparison.py`
  - `cloud_dog_vdb/testing/comparison_report.py`
- New tests added:
  - UT3.1 .. UT3.8
  - ST3.1 .. ST3.4
  - PT3.1 .. PT3.8
- Env/config updates:
  - `tests/env-ST|IT|AT|PT|PT-PERF` marker enabled + async/retry settings
  - `tests/env-PT-COMPARE` added
- Version bump completed:
  - `pyproject.toml`: `0.5.0`
  - `cloud_dog_vdb/__init__.py`: `0.5.0`
- Docs updated to reflect `0.5.0` status in `REQUIREMENTS.md`, `ARCHITECTURE.md`, `TESTS.md`

## Quality Gates (Executed)

- `ruff check cloud_dog_vdb tests` -> PASS
- `ruff format --check cloud_dog_vdb tests` -> PASS
- `pytest tests/unit/UT3.* --env tests/env-UT -q` -> PASS (`8 passed`)
- `pytest tests/system/ST3.* --env tests/env-ST -q` -> PASS (`4 passed`)
- `pytest tests/unit tests/system --env tests/env-UT --env tests/env-ST -q` -> PASS (`102 passed`)
- `pytest tests/compatibility --env tests/env-CT -q` -> PASS (`7 passed`)
- `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; pytest tests/integration --env tests/env-IT -q` -> PASS (`24 passed`)
- `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; pytest tests/application --env tests/env-AT -q` -> PASS (`7 passed`)
- `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; pytest tests/security --env tests/env-IT -q` -> PASS (`12 passed`)
- `pytest tests/parser/PT3.1_CorpusSmallComparison --env tests/env-PT-COMPARE --env tests/env-CORPUS-SMALL -q` -> PASS
- `pytest tests/parser/PT3.2_CorpusMediumComparison --env tests/env-PT-COMPARE --env tests/env-CORPUS-MEDIUM -q` -> PASS
- `timeout 3600 pytest tests/parser/PT3.3_CorpusLargeComparison --env tests/env-PT-COMPARE --env tests/env-CORPUS-LARGE -q` -> PASS
- `pytest tests/parser/PT3.* --env tests/env-PT-COMPARE --env tests/env-CORPUS-SMALL -q` -> PASS (`8 passed`)
- `python -m build` -> PASS
- `pip install --force-reinstall dist/cloud_dog_vdb-0.5.0-py3-none-any.whl` -> PASS
- `python -c "import cloud_dog_vdb; print(cloud_dog_vdb.__version__)"` -> PASS (`0.5.0`)
- `grep -Rsn "os\.environ\|os\.getenv" cloud_dog_vdb/ --include='*.py'` -> PASS (no hits)

## Open Blocker

- PT1.4 marker corpus quality gate remains unstable/failing due live Marker endpoint intermittency.

Observed failures:
- `pytest tests/parser/PT1.4_MarkerMcpAdapter --env tests/env-PT --env tests/env-CORPUS-SMALL -q` -> FAIL (`success_ratio = 0.0`, route 404/read errors)
- Medium and large slice PT1.4 runs also failed when endpoint returned 404/health false.

Evidence of service instability (same day):
- `curl https://marker0.cloud-dog.net/openapi.json` alternated between `HTTP 200` and `HTTP 404`.
- `POST /marker/upload` alternated between valid parse responses and `404 page not found`.

This is an external service availability/routing issue, not an internal parser code contract failure.

## Comparison Reports

Generated under: `tests/comparison_reports/`

- `pt3_1_corpus_small_comparison.(json|md)`
- `pt3_2_corpus_medium_comparison.(json|md)`
- `pt3_3_corpus_large_comparison.(json|md)`
- `pt3_4_marker_vs_mineru_quality.(json|md)`
- `pt3_5_provider_latency_ranking.(json|md)`
- `pt3_6_table_extraction_comparison.(json|md)`
- `pt3_7_image_extraction_comparison.(json|md)`
- `pt3_8_comparison_report_generation.(json|md)`
