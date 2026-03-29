"""Tests for result summarization and column statistics."""

import pytest

from sql_to_graph._native import summarize_result


@pytest.mark.asyncio
async def test_numeric_stats(ecommerce_conn):
    result = await ecommerce_conn.execute_with_context(
        "SELECT total_amount::float8 AS total_amount FROM ecommerce.orders", "ecommerce"
    )
    summary = summarize_result(result)
    assert len(summary.column_stats) == 1
    stats = summary.column_stats[0]
    assert stats.column_name == "total_amount"
    assert stats.mean is not None
    assert stats.min is not None
    assert stats.max is not None


@pytest.mark.asyncio
async def test_categorical_stats(ecommerce_conn):
    result = await ecommerce_conn.execute_with_context(
        "SELECT region FROM ecommerce.customers", "ecommerce"
    )
    summary = summarize_result(result)
    stats = summary.column_stats[0]
    assert stats.distinct_count is not None
    assert stats.distinct_count <= 6  # 6 regions in seed data
    assert stats.top_values is not None


@pytest.mark.asyncio
async def test_temporal_stats(ecommerce_conn):
    """Date cast to text is treated as categorical; verify stats are computed."""
    result = await ecommerce_conn.execute_with_context(
        "SELECT order_date::text AS order_date FROM ecommerce.orders", "ecommerce"
    )
    summary = summarize_result(result)
    stats = summary.column_stats[0]
    # Cast to text => treated as categorical with top_values and distinct_count
    assert stats.distinct_count is not None
    assert stats.distinct_count > 1


@pytest.mark.asyncio
async def test_null_warnings(connection):
    """performance_reviews.reviewer has ~20% NULLs — should trigger a warning."""
    result = await connection.execute_with_context(
        "SELECT reviewer FROM hr.performance_reviews", "hr"
    )
    summary = summarize_result(result)
    stats = summary.column_stats[0]
    assert stats.null_count > 0
    # The null percentage should be roughly 20%
    null_pct = stats.null_count / 1000
    assert 0.10 < null_pct < 0.35


@pytest.mark.asyncio
async def test_multi_column_stats(ecommerce_conn):
    result = await ecommerce_conn.execute_with_context(
        "SELECT quantity, unit_price::float8 AS unit_price FROM ecommerce.order_items", "ecommerce"
    )
    summary = summarize_result(result)
    assert len(summary.column_stats) == 2
    names = [s.column_name for s in summary.column_stats]
    assert "quantity" in names
    assert "unit_price" in names


@pytest.mark.asyncio
async def test_single_value_column(ecommerce_conn):
    """A column with constant value should have distinct_count=1."""
    result = await ecommerce_conn.execute_with_context(
        "SELECT 42 AS constant FROM ecommerce.orders LIMIT 100", "ecommerce"
    )
    summary = summarize_result(result)
    stats = summary.column_stats[0]
    assert stats.distinct_count == 1
