# platform-vdb Agent Guidance

This runbook is for agents implementing or validating `cloud_dog_vdb` changes.

## Non-Negotiable Rules

- Use real dependencies for `ST/IT/AT/PT/QT` tiers.
- Do not add stubs, silent fallbacks, or fake success paths to satisfy tests.
- Preserve backward compatibility:
  - public API parity,
  - default behavior parity,
  - metadata identity parity,
  - error/result envelope parity.
- Load credentials from approved env/Vault sources only.

## Required Inputs

- Repository root: `/opt/iac/Development/cloud-dog-ai/cloud-dog-ai-platform-standards`
- Package root: `packages/backend/platform-vdb`
- Vault env file (not committed): `/opt/iac/Development/cloud-dog-ai/env-vault`
- Corpus manifest: `test-data/corpus-manifest.yaml`

## Service Configuration Expectations

- `dev.services.mineru` must resolve to `MINERU_BASE_URL` for parser tests.
- `dev.services.marker_mcp` (or `dev.services.markermcp`) is supported but currently held disabled when instructed.
- `dev.vdbs.infinity` must exist for Infinity adapter integration tests.
- Local parser command adapters are supported (no Vault required) for:
  - `deepdoc` via `tests/tools/local_deepdoc_parser.py`
  - `docling` via `tests/tools/local_docling_parser.py`
  - `transformers` via `tests/tools/local_transformers_parser.py`

## Local Parser Setup

- Install local parser dependencies in package venv:
  - `.venv/bin/pip install transformers==4.57.1 docling-parse==5.4.0`
- Ensure env files used for IT/AT/PT/PT-PERF enable parser commands:
  - `DEEPDOC_ENABLED=true`, `DOCLING_ENABLED=true`, `TRANSFORMERS_ENABLED=true`
  - command values point to `.venv/bin/python tests/tools/local_*_parser.py`
- For PT/PT-PERF MinerU stability on shared GPU hosts, keep low-VRAM flags explicit:
  - `MINERU_FORMULA_ENABLE=false`
  - `MINERU_TABLE_ENABLE=false`
  - `MINERU_RETURN_MIDDLE_JSON=false`
  - `MINERU_RETURN_IMAGES=false`

## Standard Execution Order

Run from `packages/backend/platform-vdb`:

0. `.venv/bin/pip install transformers==4.57.1 docling-parse==5.4.0`
1. `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT --env tests/env-REQUIRE-ALL-PARSERS -q`
2. `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-REQUIRE-ALL-PARSERS -q`
3. `.venv/bin/pytest tests/parser/PT1.2_DeepdocAdapter tests/parser/PT1.3_DoclingAdapter tests/parser/PT1.8_TransformersAdapter --env tests/env-PT --env tests/env-CORPUS-SMALL -q`
4. `.venv/bin/ruff check`
5. `.venv/bin/ruff format --check`
6. `.venv/bin/pytest tests/unit tests/system --env tests/env-UT --env tests/env-ST -q`
7. `.venv/bin/pytest tests/compatibility --env tests/env-CT -q`
8. `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/integration --env tests/env-IT -q`
9. `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 1800 .venv/bin/pytest tests/parser --env tests/env-PT --env tests/env-CORPUS-LARGE -q`
10. `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; timeout 1800 .venv/bin/pytest tests/parser_performance --env tests/env-PT-PERF --env tests/env-CORPUS-LARGE -q`
11. `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/application --env tests/env-AT -q`
12. `set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a; .venv/bin/pytest tests/security --env tests/env-IT -q`
13. `.venv/bin/python -m build`
14. `.venv/bin/pip install --force-reinstall dist/cloud_dog_vdb-0.4.1-py3-none-any.whl`
15. Smoke import: `.venv/bin/python -c "import cloud_dog_vdb; print(cloud_dog_vdb.__version__)"`

For full evidence capture, mirror outputs to `/tmp/w13a_*.log` using `tee`.

## Staged Parser Validation (Recommended)

Use corpus slicing for deterministic progression:

- Small: `--env tests/env-CORPUS-SMALL`
- Medium: `--env tests/env-CORPUS-MEDIUM`
- Large: `--env tests/env-CORPUS-LARGE`

Recommended progression:

1. `tests/parser` + small
2. `tests/parser_performance` + small
3. `tests/parser` + medium
4. `tests/parser_performance` + medium
5. Large slice under explicit timeout guard

## Documentation Stage (Release Gate)

After test/build execution, update:

- `TESTS.md` run history (commands, date, pass/fail/skip, blockers).
- `README.md` document links and runtime prerequisites.
- `PROGRAMME-0.4.0-DEVELOPMENT-BUILD-TEST.md` if sequencing/gates changed.
- `RELEASE_UPLIFT_PROPOSAL.md` if scope or constraints changed.

Do not claim 100% completion if any parser-performance or provider enablement gate is still open.
