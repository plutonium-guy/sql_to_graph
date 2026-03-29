"""Tests for AgentMemory: remember, recall, persistence, purge."""

import json
import os

import pytest

from sql_to_graph.memory import AgentMemory, MemoryEntry


def test_remember_and_recall_query(tmp_memory):
    entry_id = tmp_memory.remember_query(
        sql="SELECT COUNT(*) FROM orders",
        intent="total order count",
        result_summary={"row_count": 1},
    )
    assert entry_id
    queries = tmp_memory.recall_queries(limit=5)
    assert len(queries) == 1
    assert queries[0].sql == "SELECT COUNT(*) FROM orders"
    assert queries[0].content == "total order count"


def test_remember_and_recall_fact(tmp_memory):
    tmp_memory.remember_fact("revenue is stored in cents", source_sql="SELECT revenue FROM orders")
    facts = tmp_memory.recall_facts()
    assert len(facts) == 1
    assert "cents" in facts[0].content


def test_remember_and_recall_preference(tmp_memory):
    tmp_memory.remember_preference("chart_format", "png")
    prefs = tmp_memory.recall_preferences()
    assert len(prefs) == 1
    assert prefs[0].metadata["key"] == "chart_format"
    assert prefs[0].metadata["value"] == "png"


def test_preference_update(tmp_memory):
    tmp_memory.remember_preference("chart_format", "png")
    tmp_memory.remember_preference("chart_format", "html")
    prefs = tmp_memory.recall_preferences()
    assert len(prefs) == 1
    assert prefs[0].metadata["value"] == "html"


def test_fuzzy_recall(tmp_memory):
    tmp_memory.remember_query(sql="SELECT * FROM orders", intent="all orders")
    tmp_memory.remember_query(sql="SELECT * FROM customers", intent="all customers")
    tmp_memory.remember_fact("orders table has 1000 rows")
    results = tmp_memory.recall("orders", limit=5)
    assert len(results) >= 2
    # Both the query and the fact mention "orders"
    contents = [r.content for r in results]
    assert any("orders" in c for c in contents)


def test_fuzzy_recall_prefers_recent_matches(tmp_memory):
    tmp_memory.remember_fact("orders fact old")
    tmp_memory.remember_fact("orders fact new")
    tmp_memory._entries[0].last_used_at = "2024-01-01T00:00:00+00:00"
    tmp_memory._entries[1].last_used_at = "2024-01-02T00:00:00+00:00"

    results = tmp_memory.recall("orders", limit=2)

    assert [r.content for r in results] == ["orders fact new", "orders fact old"]


def test_purge_single(tmp_memory):
    id1 = tmp_memory.remember_fact("fact one")
    id2 = tmp_memory.remember_fact("fact two")
    deleted = tmp_memory.purge(entry_id=id1)
    assert deleted == 1
    assert tmp_memory.size == 1
    assert tmp_memory.recall_facts()[0].id == id2


def test_purge_all(tmp_memory):
    tmp_memory.remember_fact("fact one")
    tmp_memory.remember_fact("fact two")
    tmp_memory.remember_query(sql="SELECT 1", intent="test")
    deleted = tmp_memory.purge()
    assert deleted == 3
    assert tmp_memory.size == 0


def test_persistence_save_load(tmp_path):
    path = str(tmp_path / "mem.json")
    mem = AgentMemory(path=path, max_entries=50)
    mem.remember_query(sql="SELECT 1", intent="test query")
    mem.remember_fact("test fact")
    mem.save()

    # Load into a new instance
    mem2 = AgentMemory(path=path, max_entries=50)
    assert mem2.size == 2
    assert mem2.recall_queries()[0].sql == "SELECT 1"
    assert mem2.recall_facts()[0].content == "test fact"


def test_save_creates_parent_directory(tmp_path):
    path = tmp_path / "nested" / "agent" / "mem.json"
    mem = AgentMemory(path=str(path), max_entries=50)
    mem.remember_fact("stored in nested path")

    assert path.exists()
    loaded = AgentMemory(path=str(path), max_entries=50)
    assert loaded.recall_facts()[0].content == "stored in nested path"


def test_get_context_for_prompt(tmp_memory):
    tmp_memory.remember_query(sql="SELECT COUNT(*) FROM orders", intent="order count")
    tmp_memory.remember_fact("revenue in cents")
    tmp_memory.remember_preference("format", "html")
    ctx = tmp_memory.get_context_for_prompt()
    assert "order count" in ctx
    assert "revenue in cents" in ctx
    assert "format: html" in ctx


def test_max_entries_eviction(tmp_path):
    path = str(tmp_path / "mem.json")
    mem = AgentMemory(path=path, max_entries=5)
    for i in range(10):
        mem.remember_fact(f"fact {i}")
    assert mem.size == 5
    # Oldest facts (0-4) should be evicted
    facts = mem.recall_facts()
    contents = [f.content for f in facts]
    assert "fact 0" not in contents
    assert "fact 9" in contents
