# Copyright 2026 Cloud-Dog, Viewdeck Engineering Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# cloud_dog_vdb — Shared test configuration
"""
Shared fixtures and --env enforcement for cloud_dog_vdb tests.
"""

import os
import pytest


def pytest_addoption(parser):
    """Add --env option for test environment selection."""
    parser.addoption(
        "--env",
        action="store",
        default="UT",
        help="Test environment tier: UT, ST, IT, AT, QT",
    )


@pytest.fixture(scope="session")
def env_tier(request):
    """Return the current test environment tier."""
    return request.config.getoption("--env").upper()


@pytest.fixture(scope="session")
def vault_env():
    """Validate Vault environment variables are present for integration tests."""
    required = ["VAULT_ADDR", "VAULT_TOKEN", "VAULT_MOUNT_POINT"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        pytest.skip(
            f"Vault credentials not in environment (missing: {', '.join(missing)}). "
            f"Source env-vault first: set -a; source /opt/iac/Development/cloud-dog-ai/env-vault; set +a"
        )
    return {k: os.environ[k] for k in required}


@pytest.fixture(scope="session")
def chroma_env(vault_env):
    """Validate Chroma is available for IT tests."""
    url = os.environ.get("CHROMA_BASE_URL", "")
    if not url:
        pytest.skip("CHROMA_BASE_URL not set — skipping Chroma IT tests")
    return {"base_url": url, **vault_env}


@pytest.fixture(scope="session")
def qdrant_env(vault_env):
    """Validate Qdrant is available for IT tests."""
    url = os.environ.get("QDRANT_URL", "")
    if not url:
        pytest.skip("QDRANT_URL not set — skipping Qdrant IT tests")
    return {"url": url, **vault_env}


@pytest.fixture(scope="session")
def weaviate_env(vault_env):
    """Validate Weaviate is available for IT tests."""
    url = os.environ.get("WEAVIATE_URL", "")
    if not url:
        pytest.skip("WEAVIATE_URL not set — skipping Weaviate IT tests")
    return {"url": url, **vault_env}


@pytest.fixture(scope="session")
def opensearch_env(vault_env):
    """Validate OpenSearch is available for IT tests."""
    host = os.environ.get("OPENSEARCH_HOST", "")
    if not host:
        pytest.skip("OPENSEARCH_HOST not set — skipping OpenSearch IT tests")
    return {"host": host, **vault_env}


@pytest.fixture(scope="session")
def pgvector_env(vault_env):
    """Validate PGVector is available for IT tests."""
    uri = os.environ.get("PGVECTOR_DATABASE_URI", "")
    if not uri:
        pytest.skip("PGVECTOR_DATABASE_URI not set — skipping PGVector IT tests")
    return {"database_uri": uri, **vault_env}


# ── Embedding model fixtures ───────────────────────────────────


@pytest.fixture(scope="session")
def ollama_embedding_env(vault_env):
    """Validate an Ollama embedding endpoint is available for IT tests.

    Primary: bge-m3:567m on llm2 (1024 dims).
    Falls back to nomic-embed-text on llm1 (768 dims).
    Set OLLAMA_EMBED_BASE_URL + OLLAMA_EMBED_MODEL to override.
    """
    base_url = os.environ.get("OLLAMA_EMBED_BASE_URL", "")
    model = os.environ.get("OLLAMA_EMBED_MODEL", "")
    dims = int(os.environ.get("OLLAMA_EMBED_DIMENSIONS", "0"))
    if not base_url:
        pytest.skip(
            "OLLAMA_EMBED_BASE_URL not set — skipping Ollama embedding IT tests. "
            "Vault: dev.models.ollama_bge_m3_567m_llm2.base_url"
        )
    return {
        "base_url": base_url,
        "model": model or "bge-m3:567m",
        "dimensions": dims or 1024,
        "provider": "ollama",
        **vault_env,
    }


@pytest.fixture(scope="session")
def openrouter_embedding_env(vault_env):
    """Validate an OpenRouter embedding endpoint is available for IT tests.

    Primary: baai/bge-m3 (1024 dims) — same model as Ollama BGE-M3,
    validates provider abstraction.
    """
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        pytest.skip(
            "OPENROUTER_API_KEY not set — skipping OpenRouter embedding IT tests. "
            "Vault: dev.models.openai_text_embedding_baai_bge_m3_openrouter.api_key"
        )
    model = os.environ.get("OPENROUTER_EMBED_MODEL", "baai/bge-m3")
    dims = int(os.environ.get("OPENROUTER_EMBED_DIMENSIONS", "1024"))
    return {
        "base_url": "https://openrouter.ai/api/v1",
        "api_key": api_key,
        "model": model,
        "dimensions": dims,
        "provider": "openai",
        **vault_env,
    }


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("ollama", id="ollama-embedding"),
        pytest.param("openrouter", id="openrouter-embedding"),
    ],
)
def any_embedding_env(request, vault_env):
    """Parametrised fixture: runs embedding tests against each available provider.

    Skips providers that are not configured.
    """
    provider = request.param
    if provider == "ollama":
        base_url = os.environ.get("OLLAMA_EMBED_BASE_URL", "")
        if not base_url:
            pytest.skip("OLLAMA_EMBED_BASE_URL not set")
        return {
            "base_url": base_url,
            "model": os.environ.get("OLLAMA_EMBED_MODEL", "bge-m3:567m"),
            "dimensions": int(os.environ.get("OLLAMA_EMBED_DIMENSIONS", "1024")),
            "provider": "ollama",
            **vault_env,
        }
    elif provider == "openrouter":
        api_key = os.environ.get("OPENROUTER_API_KEY", "")
        if not api_key:
            pytest.skip("OPENROUTER_API_KEY not set")
        return {
            "base_url": "https://openrouter.ai/api/v1",
            "api_key": api_key,
            "model": os.environ.get("OPENROUTER_EMBED_MODEL", "baai/bge-m3"),
            "dimensions": int(os.environ.get("OPENROUTER_EMBED_DIMENSIONS", "1024")),
            "provider": "openai",
            **vault_env,
        }
