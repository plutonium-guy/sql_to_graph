"""Tests for TOONS — Token Oriented Object Notation Serializer."""

import json

from sql_to_graph.toons import (
    ToonsConfig,
    compare_token_usage,
    estimate_tokens,
    toons_decode,
    toons_encode,
)


def _sample_result():
    """A realistic tool result dict."""
    return {
        "sql_executed": "SELECT id, name, region FROM customers LIMIT 5",
        "columns": ["id", "name", "region"],
        "row_count": 5,
        "execution_time_ms": 12.3,
        "from_cache": False,
        "rows": [
            {"id": 1, "name": "Alice", "region": "East"},
            {"id": 2, "name": "Bob", "region": "West"},
            {"id": 3, "name": "Charlie", "region": "North"},
            {"id": 4, "name": "Diana", "region": "South"},
            {"id": 5, "name": "Eve", "region": "East"},
        ],
    }


def test_encode_basic():
    data = _sample_result()
    encoded = toons_encode(data)
    assert "§sql:" in encoded
    assert "§cols:id|name|region" in encoded
    assert "§rows:" in encoded
    assert "Alice" in encoded
    assert "§meta:" in encoded


def test_encode_is_shorter_than_json():
    data = _sample_result()
    json_str = json.dumps(data)
    toons_str = toons_encode(data)
    assert len(toons_str) < len(json_str)


def test_encode_null_values():
    data = {
        "sql_executed": "SELECT x FROM t",
        "columns": ["x", "y"],
        "row_count": 2,
        "rows": [
            {"x": 1, "y": None},
            {"x": None, "y": 2},
        ],
    }
    encoded = toons_encode(data)
    assert "∅" in encoded  # null marker


def test_encode_error():
    data = {
        "error": {
            "message": "relation does not exist",
            "available_tables": ["customers", "orders"],
            "suggestions": ["customers"],
        },
        "sql_executed": "SELECT * FROM cusotmers",
    }
    encoded = toons_encode(data)
    assert "§err:" in encoded
    assert "§tables:" in encoded
    assert "§did_you_mean:" in encoded
    assert "§sql:" in encoded


def test_encode_with_stats():
    data = {
        "sql_executed": "SELECT x FROM t",
        "columns": ["x"],
        "row_count": 100,
        "rows": [{"x": i} for i in range(10)],
        "statistics": {
            "columns": [
                {
                    "name": "x",
                    "category": "numeric",
                    "min": 0,
                    "max": 99,
                    "mean": 49.5,
                    "null_count": 0,
                    "distinct_count": 100,
                }
            ],
            "warnings": ["No issues found"],
        },
    }
    encoded = toons_encode(data)
    assert "§stats:" in encoded
    assert "mean=" in encoded
    assert "§warnings:" in encoded


def test_encode_with_suggestions():
    data = {
        "sql_executed": "SELECT region, COUNT(*) FROM customers GROUP BY region",
        "columns": ["region", "count"],
        "row_count": 4,
        "rows": [
            {"region": "East", "count": 50},
            {"region": "West", "count": 40},
        ],
        "chart_suggestions": [
            {
                "chart_type": "Bar",
                "x_column": "region",
                "y_column": "count",
                "title": "Customers by Region",
                "confidence": 0.92,
                "reasoning": "categorical + numeric",
            }
        ],
    }
    encoded = toons_encode(data)
    assert "§suggest:" in encoded
    assert "Bar" in encoded
    assert "conf=0.92" in encoded


def test_encode_discovery_schemas():
    data = {
        "schemas": [
            {"name": "ecommerce", "table_count": 5},
            {"name": "hr", "table_count": 3},
        ]
    }
    encoded = toons_encode(data)
    assert "§schemas:" in encoded
    assert "ecommerce" in encoded
    assert "hr" in encoded


def test_encode_table_meta():
    data = {
        "schema": "ecommerce",
        "table": "customers",
        "row_count_estimate": 200,
        "columns": [
            {"name": "id", "data_type": "integer", "is_nullable": False},
            {"name": "name", "data_type": "varchar", "is_nullable": False},
            {"name": "email", "data_type": "varchar", "is_nullable": True},
        ],
    }
    encoded = toons_encode(data)
    assert "§table:" in encoded
    assert "ecommerce.customers" in encoded
    assert "NOT NULL" in encoded
    assert "NULL" in encoded


def test_decode_roundtrip():
    data = _sample_result()
    encoded = toons_encode(data)
    decoded = toons_decode(encoded)
    assert decoded["sql_executed"] == data["sql_executed"]
    assert decoded["columns"] == data["columns"]
    assert len(decoded["rows"]) == len(data["rows"])
    assert decoded["rows"][0]["name"] == "Alice"


def test_decode_error():
    data = {
        "error": {"message": "table not found"},
        "sql_executed": "SELECT * FROM bad",
    }
    encoded = toons_encode(data)
    decoded = toons_decode(encoded)
    assert "error" in decoded
    assert decoded["error"]["message"] == "table not found"


def test_max_rows_truncation():
    data = {
        "columns": ["x"],
        "row_count": 200,
        "rows": [{"x": i} for i in range(200)],
    }
    cfg = ToonsConfig(max_rows=10)
    encoded = toons_encode(data, config=cfg)
    assert "+190 rows" in encoded


def test_max_value_len_truncation():
    data = {
        "columns": ["text"],
        "row_count": 1,
        "rows": [{"text": "A" * 500}],
    }
    cfg = ToonsConfig(max_value_len=50)
    encoded = toons_encode(data, config=cfg)
    assert "…" in encoded


def test_compare_token_usage():
    data = _sample_result()
    comparison = compare_token_usage(data)
    assert comparison["toons_tokens"] < comparison["json_tokens"]
    assert comparison["savings_pct"] > 0
    assert comparison["toons_chars"] < comparison["json_chars"]


def test_estimate_tokens():
    assert estimate_tokens("hello world") > 0
    assert estimate_tokens("a b c d e") > estimate_tokens("abc")


def test_pipe_escaping():
    data = {
        "columns": ["text"],
        "row_count": 1,
        "rows": [{"text": "has|pipe|chars"}],
    }
    encoded = toons_encode(data)
    # Should escape the pipes
    assert "has\\|pipe\\|chars" in encoded
    # Decode should handle it
    decoded = toons_decode(encoded)
    # Basic structure should survive
    assert decoded["columns"] == ["text"]
