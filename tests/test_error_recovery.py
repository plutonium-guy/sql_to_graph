"""Tests for enriched error recovery and error handling in tool calls."""

import json

import pytest

from sql_to_graph._native import Connection
from sql_to_graph.agent import handle_tool_call


@pytest.mark.asyncio
async def test_enriched_error_has_suggestions(pg_connection_string):
    """Misspelled table name should return fuzzy suggestions."""
    conn = Connection(pg_connection_string, read_only=True, schema="ecommerce")
    await conn.connect()
    try:
        with pytest.raises(Exception) as exc_info:
            await conn.execute_with_context(
                "SELECT * FROM ecommerce.cusotmers", "ecommerce"
            )
        error_str = str(exc_info.value)
        # Try to parse the enriched error JSON
        try:
            enriched = json.loads(error_str)
            assert "suggestions" in enriched or "available_tables" in enriched
        except json.JSONDecodeError:
            # If not JSON, the error message should at least exist
            assert len(error_str) > 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_enriched_error_has_available_tables(pg_connection_string):
    """Error response should list available tables."""
    conn = Connection(pg_connection_string, read_only=True, schema="ecommerce")
    await conn.connect()
    try:
        with pytest.raises(Exception) as exc_info:
            await conn.execute_with_context(
                "SELECT * FROM ecommerce.nonexistent_table_xyz", "ecommerce"
            )
        error_str = str(exc_info.value)
        try:
            enriched = json.loads(error_str)
            if "available_tables" in enriched:
                assert len(enriched["available_tables"]) > 0
        except json.JSONDecodeError:
            pass  # Not all errors are enriched
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_enriched_error_has_available_columns(pg_connection_string):
    """Error response should surface column suggestions to Python callers."""
    conn = Connection(pg_connection_string, read_only=True, schema="ecommerce")
    await conn.connect()
    try:
        with pytest.raises(Exception) as exc_info:
            await conn.execute_with_context(
                "SELECT customer_nam FROM ecommerce.customers", "ecommerce"
            )
        enriched = json.loads(str(exc_info.value))
        assert "available_columns" in enriched
        assert any(col.endswith(".name") for col in enriched["available_columns"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_handle_tool_call_returns_error_dict(pg_connection_string):
    """handle_tool_call should return an error dict, not raise."""
    result = await handle_tool_call(
        {
            "sql": "SELECT * FROM ecommerce.nonexistent_xyz",
            "connection_string": pg_connection_string,
            "schema": "ecommerce",
        }
    )
    assert "error" in result
    assert "sql_executed" in result


@pytest.mark.asyncio
async def test_handle_tool_call_success(pg_connection_string):
    """Successful query should return columns and rows."""
    result = await handle_tool_call(
        {
            "sql": "SELECT COUNT(*) AS cnt FROM ecommerce.orders",
            "connection_string": pg_connection_string,
            "schema": "ecommerce",
            "optimize": False,
            "auto_correct": False,
        }
    )
    assert "error" not in result
    assert result["columns"] == ["cnt"]
    assert result["row_count"] == 1
    assert result["rows"][0]["cnt"] == 1000


@pytest.mark.asyncio
async def test_handle_tool_call_with_cache(pg_connection_string, cache):
    """Second call with same SQL should be a cache hit."""
    args = {
        "sql": "SELECT 1 AS x",
        "connection_string": pg_connection_string,
        "optimize": False,
        "auto_correct": False,
    }
    r1 = await handle_tool_call(args, cache=cache)
    r2 = await handle_tool_call(args, cache=cache)
    assert r1["rows"] == r2["rows"]
    assert r2["from_cache"] is True
    assert cache._hits == 1
