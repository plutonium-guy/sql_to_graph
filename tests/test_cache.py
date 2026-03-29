"""Tests for QueryCache: hits, misses, normalization, eviction, context isolation."""

import pytest

from sql_to_graph.cache import QueryCache


class FakeResult:
    """Minimal stand-in for QueryResult."""
    def __init__(self, label="result"):
        self.label = label
        self.columns = ["a"]
        self.row_count = 1
        self.rows = []
        self.execution_time_ms = 0.0
        self.total_row_count = None
        self.has_more = False


def test_cache_hit():
    cache = QueryCache(max_size=10)
    r = FakeResult("first")
    cache.put("SELECT 1", r)
    assert cache.get("SELECT 1") is r
    assert cache._hits == 1


def test_cache_miss():
    cache = QueryCache(max_size=10)
    assert cache.get("SELECT 1") is None
    assert cache._misses == 1


def test_cache_normalization():
    """Queries differing only in whitespace/case should be the same cache key."""
    cache = QueryCache(max_size=10)
    r = FakeResult()
    cache.put("  SELECT  1  ", r)
    assert cache.get("select 1") is r


def test_cache_eviction():
    cache = QueryCache(max_size=3)
    for i in range(5):
        cache.put(f"SELECT {i}", FakeResult(str(i)))
    assert cache.size == 3
    # Oldest entries (0 and 1) should be evicted
    assert cache.get("SELECT 0") is None
    assert cache.get("SELECT 1") is None
    assert cache.get("SELECT 2") is not None


def test_cache_lru_order():
    cache = QueryCache(max_size=3)
    cache.put("SELECT 1", FakeResult("1"))
    cache.put("SELECT 2", FakeResult("2"))
    cache.put("SELECT 3", FakeResult("3"))
    # Access "SELECT 1" to make it most recently used
    cache.get("SELECT 1")
    # Add a new entry — should evict "SELECT 2" (least recently used)
    cache.put("SELECT 4", FakeResult("4"))
    assert cache.get("SELECT 1") is not None
    assert cache.get("SELECT 2") is None


def test_cache_context_isolation():
    """Same SQL with different contexts should be separate cache entries."""
    cache = QueryCache(max_size=10)
    r1 = FakeResult("db1")
    r2 = FakeResult("db2")
    cache.put("SELECT 1", r1, context="postgresql://db1")
    cache.put("SELECT 1", r2, context="postgresql://db2")
    assert cache.get("SELECT 1", context="postgresql://db1") is r1
    assert cache.get("SELECT 1", context="postgresql://db2") is r2
    # Without context, should miss
    assert cache.get("SELECT 1") is None


def test_cache_clear():
    cache = QueryCache(max_size=10)
    cache.put("SELECT 1", FakeResult())
    cache.put("SELECT 2", FakeResult())
    cache.clear()
    assert cache.size == 0
    assert cache.hit_rate == 0.0
