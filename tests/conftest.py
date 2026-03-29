"""Shared fixtures for sql_to_graph tests."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from sql_to_graph._native import Connection
from sql_to_graph.cache import QueryCache
from sql_to_graph.memory import AgentMemory


# ─── PostgreSQL connection ────────────────────────────────────────────────

PG_CONN = os.environ.get(
    "TEST_PG_CONNECTION",
    "postgresql://postgres:testpassword@localhost:15432/testdb",
)


@pytest.fixture(scope="session")
def pg_connection_string() -> str:
    return PG_CONN


@pytest.fixture(scope="session", autouse=True)
def seed_database(pg_connection_string: str):
    """Run seed_pg.sql once per test session via docker exec."""
    seed_file = os.path.join(os.path.dirname(__file__), "seed_pg.sql")
    if not os.path.exists(seed_file):
        pytest.skip("seed_pg.sql not found")
    with open(seed_file) as f:
        sql = f.read()
    # Try psql directly first, fall back to docker exec
    try:
        subprocess.run(
            ["psql", pg_connection_string, "-f", seed_file],
            check=True,
            capture_output=True,
        )
    except (FileNotFoundError, NotADirectoryError, OSError):
        subprocess.run(
            [
                "docker", "exec", "-i", "sql_to_graph_test_pg",
                "psql", "-U", "postgres", "-d", "testdb",
            ],
            input=sql,
            text=True,
            check=True,
            capture_output=True,
        )


@pytest_asyncio.fixture
async def connection(pg_connection_string: str):
    """Read-only Connection, auto-closed."""
    conn = Connection(pg_connection_string, read_only=True)
    await conn.connect()
    yield conn
    await conn.close()


@pytest_asyncio.fixture
async def ecommerce_conn(pg_connection_string: str):
    """Connection scoped to ecommerce schema."""
    conn = Connection(pg_connection_string, read_only=True, schema="ecommerce")
    await conn.connect()
    yield conn
    await conn.close()


# ─── Cache & Memory ──────────────────────────────────────────────────────

@pytest.fixture
def cache() -> QueryCache:
    return QueryCache(max_size=10)


@pytest.fixture
def tmp_memory(tmp_path) -> AgentMemory:
    path = str(tmp_path / "test_memory.json")
    return AgentMemory(path=path, max_entries=50)


# ─── Mock LLM ────────────────────────────────────────────────────────────

@pytest.fixture
def mock_llm():
    """LLMProvider mock that returns the input SQL unchanged."""
    llm = MagicMock()
    llm.complete = AsyncMock(side_effect=lambda prompt: prompt.split("```sql")[-1].split("```")[0].strip() if "```sql" in prompt else "SELECT 1")
    return llm


# ─── Mock Anthropic client ───────────────────────────────────────────────

@dataclass
class _ToolUseBlock:
    type: str = "tool_use"
    id: str = "tu_001"
    name: str = ""
    input: dict = field(default_factory=dict)


@dataclass
class _TextBlock:
    type: str = "text"
    text: str = ""


@dataclass
class _AnthropicResponse:
    content: list = field(default_factory=list)


def make_anthropic_response(
    text: str = "",
    tool_calls: list[dict] | None = None,
) -> _AnthropicResponse:
    """Build a fake Anthropic messages.create() response."""
    blocks: list = []
    if text:
        blocks.append(_TextBlock(text=text))
    if tool_calls:
        for i, tc in enumerate(tool_calls):
            blocks.append(_ToolUseBlock(
                id=f"tu_{i:03d}",
                name=tc["name"],
                input=tc["input"],
            ))
    return _AnthropicResponse(content=blocks)


@pytest.fixture
def mock_anthropic_client():
    """AsyncAnthropic-like mock with messages.create()."""
    client = MagicMock()
    client.messages = MagicMock()
    client.messages.create = AsyncMock()
    return client


# ─── Event collector ─────────────────────────────────────────────────────

@pytest.fixture
def event_collector():
    """Returns (callback, events_list) for on_event tracking."""
    events: list = []

    def collector(event):
        events.append(event)

    return collector, events
