# W23A Parser Env Setup Report

## Scope
- Project primary: `packages/backend/platform-vdb`
- Follow-on validation: `index-retriever-mcp-server` parser integration gating
- Goal: configure and validate 5 non-internal parser providers (DeepDoc, Docling, Transformers, MinerU, Marker MCP)

## Phase 1: Local parser dependency install
Command:
```bash
.venv/bin/pip install transformers==4.57.1 docling-parse==5.4.0
```
Result:
- `transformers==4.57.1` already installed
- `docling-parse==5.4.0` already installed
- Evidence: `working/W23A-PARSER-SETUP-install.log`

## Phase 2: Env overlay configured
Created/updated:
- `tests/env-REQUIRE-ALL-PARSERS`

Configured values:
- `REQUIRE_ALL_PDF_PARSERS=true`
- `DEEPDOC_ENABLED=true`
- `DEEPDOC_COMMAND=.venv/bin/python tests/tools/local_deepdoc_parser.py`
- `DOCLING_ENABLED=true`
- `DOCLING_COMMAND=.venv/bin/python tests/tools/local_docling_parser.py`
- `TRANSFORMERS_ENABLED=true`
- `TRANSFORMERS_COMMAND=.venv/bin/python tests/tools/local_transformers_parser.py`
- `MINERU_ENABLED=true`
- `MINERU_FORMULA_ENABLE=false`
- `MINERU_TABLE_ENABLE=false`
- `MINERU_RETURN_MIDDLE_JSON=false`
- `MINERU_RETURN_IMAGES=false`
- `MARKER_MCP_ENABLED=true`

## Phase 3: Service verification
- `curl -sf https://marker0.cloud-dog.net/health` succeeded.
- Marker health was reachable throughout (status `ok`), but often `busy=true` with single worker.
- `MINERU_BASE_URL` was not exported by shell vault environment in this session; platform-vdb env files already include MinerU endpoint values used by tests.

## Phase 4: platform-vdb IT/AT/PT matrix results
Commands run with `.venv/bin/python` and real services.

1. IT coverage matrix
```bash
pytest tests/integration/IT2.11_ParserProviderCoverageMatrix --env tests/env-IT --env tests/env-REQUIRE-ALL-PARSERS -v --tb=short
```
Result:
- `1 passed in 40.15s`
- Evidence: `working/W23A-PARSER-IT.log`

2. AT coverage matrix
```bash
pytest tests/application/AT2.2_ParserProviderCoverageMatrix --env tests/env-AT --env tests/env-REQUIRE-ALL-PARSERS -v --tb=short
```
Result:
- `1 passed in 26.36s`
- Evidence: `working/W23A-PARSER-AT.log`

3. PT local parser adapters
```bash
pytest tests/parser/PT1.2_DeepdocAdapter tests/parser/PT1.3_DoclingAdapter tests/parser/PT1.8_TransformersAdapter --env tests/env-PT --env tests/env-CORPUS-SMALL -v --tb=short
```
Result:
- `3 passed in 38.88s`
- Evidence: `working/W23A-PARSER-PT.log`

4. Marker parser explicit check
```bash
pytest tests/parser/PT1.4_MarkerMcpAdapter --env tests/env-PT --env tests/env-CORPUS-SMALL --env tests/env-REQUIRE-ALL-PARSERS -v --tb=short
```
Result:
- `1 xfailed in 0.53s`
- Evidence: `working/W23A-PARSER-MARKER-PT14.log`

## Per-parser status (platform-vdb)
- DeepDoc: configured, validated (PT pass + IT/AT coverage pass)
- Docling: configured, validated (PT pass + IT/AT coverage pass)
- Transformers: configured, validated (PT pass + IT/AT coverage pass)
- MinerU: configured, validated by IT/AT coverage matrix pass
- Marker MCP: configured and reachable; PT1.4 currently xfails due provider busy state (not disabled, real-service gate outcome)

## Follow-on: index-retriever parser env setup and validation
Changes made:
- Created `/opt/iac/Development/cloud-dog-ai/index-retriever-mcp-server/tests/env-REQUIRE-ALL-PARSERS`
- Installed missing runtime deps required for parser integration execution in that project venv:
  - `hvac`
  - `asyncpg`

Index-retriever runs:
1. Initial run after env setup
- `4 passed, 1 failed, 1 error`
- Failure: marker busy (`OCR worker busy; queue wait exceeded 30s`)
- Error: missing `asyncpg` (resolved by install)
- Evidence: `/opt/iac/Development/cloud-dog-ai/index-retriever-mcp-server/working/W23A-PARSER-SETUP-index-retriever-IT.log`

2. Rerun after `asyncpg` install
- `1 passed, 1 failed` (subset IT2_10 + IT2_11)
- Remaining failure: marker busy (`OCR worker busy; queue wait exceeded 30s`)
- Evidence: `/opt/iac/Development/cloud-dog-ai/index-retriever-mcp-server/working/W23A-PARSER-SETUP-index-retriever-IT-rerun.log`

## Current blocker
- Marker MCP remains capacity-limited in this window (`workers=1`, frequent `busy=true`), causing real parser requests to fail with queue-wait timeout.
