"""Tests for DataAnalystAgent: full loop with mocked LLM."""

import json

import pytest

from sql_to_graph.cache import QueryCache
from sql_to_graph.memory import AgentMemory
from sql_to_graph.react_agent import (
    AgentResponse,
    DataAnalystAgent,
    RoundEvent,
    ToolCallEvent,
    build_schema_ddl,
)
from tests.conftest import make_anthropic_response, make_chat_with_tools_result


# ─── Schema DDL ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_build_schema_ddl(pg_connection_string):
    ddl = await build_schema_ddl(pg_connection_string)
    assert "ecommerce" in ddl
    assert "hr" in ddl
    assert "analytics" in ddl
    assert "customers" in ddl
    assert "orders" in ddl


# ─── Agent with mock Anthropic client ────────────────────────────────────

@pytest.mark.asyncio
async def test_simple_query_flow(pg_connection_string, mock_anthropic_client, event_collector):
    """Agent executes a tool call and returns a final text response."""
    callback, events = event_collector

    # Round 1: LLM requests a tool call
    mock_anthropic_client.messages.create.side_effect = [
        make_anthropic_response(
            tool_calls=[{
                "name": "sql_to_graph",
                "input": {
                    "sql": "SELECT COUNT(*) AS cnt FROM ecommerce.orders",
                    "include_stats": False,
                    "suggest_charts": False,
                    "optimize": False,
                    "auto_correct": False,
                },
            }],
        ),
        # Round 2: LLM returns final text
        make_anthropic_response(text="There are 1000 orders in the database."),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm_client=mock_anthropic_client,
        model="claude-sonnet-4-20250514",
        provider_type="anthropic",
        on_event=callback,
    )
    response = await agent.chat("How many orders are there?")

    assert isinstance(response, AgentResponse)
    assert "1000" in response.text
    assert response.rounds_used == 2
    assert len(response.tool_calls) == 1
    assert response.tool_calls[0].tool_name == "sql_to_graph"
    assert response.tool_calls[0].error is None

    # Check events were emitted
    tool_events = [e for e in events if isinstance(e, ToolCallEvent)]
    round_events = [e for e in events if isinstance(e, RoundEvent)]
    assert len(tool_events) == 1
    assert len(round_events) >= 1


@pytest.mark.asyncio
async def test_discovery_flow(pg_connection_string, mock_anthropic_client):
    """Agent can call discovery tools."""
    mock_anthropic_client.messages.create.side_effect = [
        make_anthropic_response(
            tool_calls=[{
                "name": "sql_discover_schemas",
                "input": {},
            }],
        ),
        make_anthropic_response(text="Found schemas: ecommerce, hr, analytics."),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm_client=mock_anthropic_client,
        model="claude-sonnet-4-20250514",
        provider_type="anthropic",
    )
    response = await agent.chat("What schemas are available?")
    assert "schemas" in response.text.lower() or response.rounds_used >= 1


@pytest.mark.asyncio
async def test_error_retry_flow(pg_connection_string, mock_anthropic_client):
    """Agent handles a failed SQL query and the LLM retries."""
    mock_anthropic_client.messages.create.side_effect = [
        # Round 1: bad SQL
        make_anthropic_response(
            tool_calls=[{
                "name": "sql_to_graph",
                "input": {
                    "sql": "SELECT * FROM ecommerce.nonexistent_xyz",
                    "optimize": False,
                    "auto_correct": False,
                },
            }],
        ),
        # Round 2: LLM fixes the SQL
        make_anthropic_response(
            tool_calls=[{
                "name": "sql_to_graph",
                "input": {
                    "sql": "SELECT COUNT(*) AS cnt FROM ecommerce.orders",
                    "optimize": False,
                    "auto_correct": False,
                },
            }],
        ),
        # Round 3: final answer
        make_anthropic_response(text="There are 1000 orders."),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm_client=mock_anthropic_client,
        model="claude-sonnet-4-20250514",
        provider_type="anthropic",
    )
    response = await agent.chat("How many orders?")
    assert response.rounds_used == 3
    assert len(response.errors) >= 1  # First query should have errored
    assert len(response.tool_calls) == 2


@pytest.mark.asyncio
async def test_max_rounds_limit(pg_connection_string, mock_anthropic_client):
    """Agent stops after max_tool_rounds even if LLM keeps calling tools."""
    # Always return a tool call, never a final text
    mock_anthropic_client.messages.create.return_value = make_anthropic_response(
        tool_calls=[{
            "name": "sql_to_graph",
            "input": {
                "sql": "SELECT 1",
                "optimize": False,
                "auto_correct": False,
            },
        }],
    )

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm_client=mock_anthropic_client,
        model="claude-sonnet-4-20250514",
        provider_type="anthropic",
        max_tool_rounds=3,
    )
    response = await agent.chat("Loop forever")
    assert response.rounds_used == 3
    assert "maximum" in response.text.lower()


@pytest.mark.asyncio
async def test_cache_across_rounds(pg_connection_string, mock_anthropic_client):
    """Same SQL in different rounds should hit the cache."""
    cache = QueryCache(max_size=10)

    mock_anthropic_client.messages.create.side_effect = [
        make_anthropic_response(
            tool_calls=[{
                "name": "sql_to_graph",
                "input": {
                    "sql": "SELECT COUNT(*) FROM ecommerce.orders",
                    "optimize": False,
                    "auto_correct": False,
                },
            }],
        ),
        # Same query again
        make_anthropic_response(
            tool_calls=[{
                "name": "sql_to_graph",
                "input": {
                    "sql": "SELECT COUNT(*) FROM ecommerce.orders",
                    "optimize": False,
                    "auto_correct": False,
                },
            }],
        ),
        make_anthropic_response(text="Done."),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm_client=mock_anthropic_client,
        model="claude-sonnet-4-20250514",
        provider_type="anthropic",
        cache=cache,
    )
    response = await agent.chat("Count orders twice")
    assert cache._hits >= 1


@pytest.mark.asyncio
async def test_memory_auto_remembers(pg_connection_string, mock_anthropic_client, tmp_path):
    """Successful queries are automatically stored in memory."""
    memory = AgentMemory(path=str(tmp_path / "mem.json"))

    mock_anthropic_client.messages.create.side_effect = [
        make_anthropic_response(
            tool_calls=[{
                "name": "sql_to_graph",
                "input": {
                    "sql": "SELECT COUNT(*) AS cnt FROM ecommerce.orders",
                    "optimize": False,
                    "auto_correct": False,
                },
            }],
        ),
        make_anthropic_response(text="1000 orders."),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm_client=mock_anthropic_client,
        model="claude-sonnet-4-20250514",
        provider_type="anthropic",
        memory=memory,
    )
    await agent.chat("How many orders?")
    assert memory.size >= 1
    queries = memory.recall_queries()
    assert any("orders" in (q.sql or "").lower() for q in queries)


@pytest.mark.asyncio
async def test_recall_tool(pg_connection_string, mock_anthropic_client, tmp_path):
    """sql_recall_queries tool returns past queries from memory."""
    memory = AgentMemory(path=str(tmp_path / "mem.json"))
    memory.remember_query(
        sql="SELECT region, COUNT(*) FROM customers GROUP BY region",
        intent="customers by region",
    )

    mock_anthropic_client.messages.create.side_effect = [
        make_anthropic_response(
            tool_calls=[{
                "name": "sql_recall_queries",
                "input": {"query": "region customers"},
            }],
        ),
        make_anthropic_response(text="Found a prior query about customers by region."),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm_client=mock_anthropic_client,
        model="claude-sonnet-4-20250514",
        provider_type="anthropic",
        memory=memory,
    )
    response = await agent.chat("Show customers by region again")
    # The recall tool should have been called and returned results
    recall_calls = [tc for tc in response.tool_calls if tc.tool_name == "sql_recall_queries"]
    assert len(recall_calls) == 1
    result = recall_calls[0].result
    assert len(result["queries"]) >= 1


@pytest.mark.asyncio
async def test_reset_clears_history(pg_connection_string, mock_anthropic_client):
    """reset() should clear conversation history."""
    mock_anthropic_client.messages.create.side_effect = [
        make_anthropic_response(text="Hello."),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm_client=mock_anthropic_client,
        model="claude-sonnet-4-20250514",
        provider_type="anthropic",
    )
    await agent.chat("Hi")
    assert len(agent._history) > 0
    agent.reset()
    assert len(agent._history) == 0


# ─── UnifiedLLM path ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_unified_llm_flow(pg_connection_string, mock_unified_llm, event_collector):
    """Agent works with UnifiedLLM instead of legacy llm_client."""
    callback, events = event_collector

    mock_unified_llm.chat_with_tools.side_effect = [
        make_chat_with_tools_result(
            tool_calls=[{
                "name": "sql_to_graph",
                "input": {
                    "sql": "SELECT COUNT(*) AS cnt FROM ecommerce.orders",
                    "include_stats": False,
                    "suggest_charts": False,
                    "optimize": False,
                    "auto_correct": False,
                },
            }],
        ),
        make_chat_with_tools_result(text="There are 1000 orders."),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm=mock_unified_llm,
        on_event=callback,
    )
    response = await agent.chat("How many orders?")

    assert isinstance(response, AgentResponse)
    assert "1000" in response.text
    assert response.rounds_used == 2
    assert len(response.tool_calls) == 1


@pytest.mark.asyncio
async def test_unified_llm_toons_encoding(pg_connection_string, mock_unified_llm):
    """TOONS encoding is used for tool results when use_toons=True."""
    mock_unified_llm.chat_with_tools.side_effect = [
        make_chat_with_tools_result(
            tool_calls=[{
                "name": "sql_to_graph",
                "input": {
                    "sql": "SELECT COUNT(*) AS cnt FROM ecommerce.orders",
                    "optimize": False,
                    "auto_correct": False,
                },
            }],
        ),
        make_chat_with_tools_result(text="Done."),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm=mock_unified_llm,
        use_toons=True,
    )
    await agent.chat("count orders")

    # Check that format_tool_results was called with TOONS-encoded content
    call_args = mock_unified_llm.format_tool_results.call_args
    tool_results = call_args[0][0]
    assert len(tool_results) == 1
    content = tool_results[0].content
    assert "§" in content  # TOONS section marker


@pytest.mark.asyncio
async def test_unified_llm_json_encoding(pg_connection_string, mock_unified_llm):
    """JSON encoding is used when use_toons=False."""
    mock_unified_llm.chat_with_tools.side_effect = [
        make_chat_with_tools_result(
            tool_calls=[{
                "name": "sql_to_graph",
                "input": {
                    "sql": "SELECT COUNT(*) AS cnt FROM ecommerce.orders",
                    "optimize": False,
                    "auto_correct": False,
                },
            }],
        ),
        make_chat_with_tools_result(text="Done."),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm=mock_unified_llm,
        use_toons=False,
    )
    await agent.chat("count orders")

    call_args = mock_unified_llm.format_tool_results.call_args
    tool_results = call_args[0][0]
    content = tool_results[0].content
    assert "§" not in content  # no TOONS markers
    assert '"sql_executed"' in content  # JSON format


@pytest.mark.asyncio
async def test_chat_isolated_independent_history(pg_connection_string, mock_unified_llm):
    """chat_isolated uses separate history, safe for parallel calls."""
    mock_unified_llm.chat_with_tools.side_effect = [
        make_chat_with_tools_result(text="Answer 1"),
        make_chat_with_tools_result(text="Answer 2"),
    ]

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm=mock_unified_llm,
    )
    # Bootstrap schema first
    await agent._bootstrap_schema()

    r1 = await agent.chat_isolated("Question 1")
    r2 = await agent.chat_isolated("Question 2")

    assert r1.text == "Answer 1"
    assert r2.text == "Answer 2"
    # Main history should be empty (isolated calls don't affect it)
    assert len(agent._history) == 0


@pytest.mark.asyncio
async def test_schema_ddl_property(pg_connection_string, mock_unified_llm):
    """schema_ddl property is accessible after bootstrap."""
    mock_unified_llm.chat_with_tools.return_value = make_chat_with_tools_result(
        text="Hello"
    )

    agent = DataAnalystAgent(
        connection_string=pg_connection_string,
        llm=mock_unified_llm,
    )
    assert agent.schema_ddl is None
    await agent.chat("hi")
    assert agent.schema_ddl is not None
    assert "ecommerce" in agent.schema_ddl
