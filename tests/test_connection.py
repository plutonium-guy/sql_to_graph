"""Tests for database connection, schema discovery, and metadata."""

import pytest

from sql_to_graph._native import Connection


@pytest.mark.asyncio
async def test_list_schemas(connection):
    schemas = await connection.list_schemas()
    schema_names = [s.name for s in schemas]
    assert "ecommerce" in schema_names
    assert "hr" in schema_names
    assert "analytics" in schema_names


@pytest.mark.asyncio
async def test_describe_table(connection):
    meta = await connection.describe_table("customers", "ecommerce")
    col_names = [c.name for c in meta.columns]
    assert "id" in col_names
    assert "name" in col_names
    assert "email" in col_names
    assert "region" in col_names


@pytest.mark.asyncio
async def test_sample_data(connection):
    result = await connection.sample_table("products", 5, "ecommerce")
    assert result.row_count <= 5
    assert "name" in result.columns
    assert "price" in result.columns


@pytest.mark.asyncio
async def test_get_metadata(connection):
    metadata = await connection.get_metadata("ecommerce")
    table_names = [m.table_name for m in metadata]
    assert "customers" in table_names
    assert "orders" in table_names
    assert "products" in table_names
    assert "order_items" in table_names
    assert "monthly_revenue" in table_names


@pytest.mark.asyncio
async def test_cross_schema_isolation(pg_connection_string):
    """Queries scoped to one schema should not see tables from another."""
    conn = Connection(pg_connection_string, read_only=True, schema="hr")
    await conn.connect()
    try:
        metadata = await conn.get_metadata("hr")
        table_names = [m.table_name for m in metadata]
        assert "departments" in table_names
        assert "employees" in table_names
        # ecommerce tables should not appear
        assert "customers" not in table_names
        assert "orders" not in table_names
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_row_count_estimate(connection):
    """ANALYZE was run, so row_count_estimate should be populated."""
    metadata = await connection.get_metadata("ecommerce")
    orders_meta = next(m for m in metadata if m.table_name == "orders")
    # We seeded 1000 rows; estimate should be in the ballpark
    assert orders_meta.row_count_estimate is not None
    assert orders_meta.row_count_estimate > 500


@pytest.mark.asyncio
async def test_connect_is_idempotent(pg_connection_string):
    """Calling connect() twice should reuse the existing pool safely."""
    conn = Connection(pg_connection_string, read_only=True, schema="ecommerce")
    await conn.connect()
    await conn.connect()
    try:
        result = await conn.execute_with_context("SELECT 1 AS x", "ecommerce")
        assert result.columns == ["x"]
        assert result.rows[0][0].to_python() == 1
    finally:
        await conn.close()
