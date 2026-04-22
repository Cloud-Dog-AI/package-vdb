# cloud-dog-vdb

**Part of the [Cloud-Dog AI Platform](https://www.cloud-dog.ai)**

> Intelligent automation through composable AI agents, MCP servers, and shared platform services.

## About Cloud-Dog AI

Cloud-Dog AI is a platform of 10+ composable services for AI-powered business automation — natural language SQL queries, email management, file operations, git workflows, notification delivery, and expert knowledge retrieval. All services share a common set of platform packages for configuration, logging, authentication, job queues, LLM integration, vector databases, and caching.

This package provides: vector database operations — embedding storage, similarity search, and document indexing across Infinity, pgvector, and other backends.

## Installation

```bash
pip install cloud-dog-vdb
```

Available from the [Cloud-Dog AI package registry](https://www.cloud-dog.ai/packages).

## Quick Start

```python
from cloud_dog_vdb import *

# See API Reference below for available functions and classes
```

## API Reference

### Exports

```python
from cloud_dog_vdb import (
    CapabilityDescriptor,
    CollectionSpec,
    IngestionPipeline,
    Job,
    ParserIngestionOptions,
    Record,
    SearchRequest,
    SearchResponse,
    SearchResult,
    VDBClient,
    annotations,
    get_vdb_client,
    ingest_document,
)
```

## Dependencies

- `httpx>=0.27`
- `pypdf>=4.0`
- `sqlalchemy>=2.0`

## Testing

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run unit tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=cloud_dog_vdb --cov-report=term-missing
```

### Test Structure
- `tests/unit/` — Unit tests (no external dependencies)
- `tests/integration/` — Integration tests (requires running services)

## Related Packages

| Package | Description |
|---------|-------------|
| cloud-dog-config | Layered configuration with Vault integration |
| cloud-dog-logging | Structured JSON logging with correlation IDs |
| cloud-dog-api-kit | FastAPI toolkit with middleware and routing |
| cloud-dog-idam | Identity and access management client |
| cloud-dog-jobs | Background job scheduling and execution |
| cloud-dog-llm | LLM client abstraction (OpenAI, Ollama, etc.) |
| cloud-dog-vdb | Vector database client (Infinity, pgvector) |
| cloud-dog-cache | Caching abstraction with Redis/Valkey support |
| cloud-dog-tokens | Design tokens for UI consistency |
| cloud-dog-ui | React component library |
| cloud-dog-shell | Application shell and navigation |
| cloud-dog-auth | Frontend authentication flows |
| cloud-dog-api-client | TypeScript API client |
| cloud-dog-config-fe | Frontend configuration management |
| cloud-dog-testing | Test utilities and fixtures |

## Version

0.5.3

---

## Licence

Apache 2.0

Copyright 2026 [Cloud-Dog](https://www.cloud-dog.ai), Viewdeck Engineering Limited ([viewdeck.io](https://www.viewdeck.io))

[info@cloud-dog.ai](mailto:info@cloud-dog.ai)
