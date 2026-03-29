"""Tests for query execution, pagination, and read-only enforcement."""

import pytest

from sql_to_graph._native import Connection


@pytest.mark.asyncio
async def test_execute_simple(ecommerce_conn):
    result = await ecommerce_conn.execute_with_context(
        "SELECT COUNT(*) AS cnt FROM ecommerce.orders", "ecommerce"
    )
    assert result.row_count == 1
    assert result.columns == ["cnt"]
    count_val = result.rows[0][0].to_python()
    assert count_val == 1000


@pytest.mark.asyncio
async def test_execute_with_context_returns_columns(ecommerce_conn):
    result = await ecommerce_conn.execute_with_context(
        "SELECT id, name, region FROM ecommerce.customers LIMIT 3", "ecommerce"
    )
    assert result.columns == ["id", "name", "region"]
    assert result.row_count == 3


@pytest.mark.asyncio
async def test_execute_paginated(ecommerce_conn):
    result = await ecommerce_conn.execute_paginated(
        "SELECT id, customer_id, status FROM ecommerce.orders ORDER BY id", 10, 0
    )
    assert result.row_count == 10
    assert result.total_row_count == 1000
    assert result.has_more is True


@pytest.mark.asyncio
async def test_execute_paginated_offset(ecommerce_conn):
    page1 = await ecommerce_conn.execute_paginated(
        "SELECT id FROM ecommerce.orders ORDER BY id", 5, 0
    )
    page2 = await ecommerce_conn.execute_paginated(
        "SELECT id FROM ecommerce.orders ORDER BY id", 5, 5
    )
    ids1 = [row[0].to_python() for row in page1.rows]
    ids2 = [row[0].to_python() for row in page2.rows]
    # Pages should not overlap
    assert set(ids1).isdisjoint(set(ids2))


@pytest.mark.asyncio
async def test_read_only_blocks_writes(pg_connection_string):
    conn = Connection(pg_connection_string, read_only=True)
    await conn.connect()
    try:
        with pytest.raises(Exception, match="(?i)(read.only|permission|denied|cannot)"):
            await conn.execute_with_context(
                "INSERT INTO ecommerce.customers (name, email, region, segment, created_at) "
                "VALUES ('Test', 'test@test.com', 'North', 'Consumer', '2024-01-01')",
                "ecommerce",
            )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_execute_join(ecommerce_conn):
    result = await ecommerce_conn.execute_with_context(
        """SELECT c.name, COUNT(o.id) AS order_count
           FROM ecommerce.customers c
           JOIN ecommerce.orders o ON o.customer_id = c.id
           GROUP BY c.name
           ORDER BY order_count DESC
           LIMIT 5""",
        "ecommerce",
    )
    assert result.row_count == 5
    assert "name" in result.columns
    assert "order_count" in result.columns
