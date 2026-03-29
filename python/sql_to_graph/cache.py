"""Simple query cache to avoid re-executing identical queries."""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sql_to_graph._native import QueryResult


class QueryCache:
    """LRU cache for query results. Avoids re-executing identical SQL.

    The optional ``context`` parameter on :meth:`get` and :meth:`put`
    isolates cache entries by connection string or schema, preventing
    cross-database cache collisions.
    """

    def __init__(self, max_size: int = 100):
        self._cache: OrderedDict[str, QueryResult] = OrderedDict()
        self._max_size = max_size
        self._hits = 0
        self._misses = 0

    def _make_key(self, sql: str, context: str | None = None) -> str:
        normalized = self._normalize(sql)
        if context:
            ctx_hash = hashlib.sha256(context.encode()).hexdigest()[:16]
            return f"{ctx_hash}:{normalized}"
        return normalized

    def get(self, sql: str, context: str | None = None) -> QueryResult | None:
        key = self._make_key(sql, context)
        if key in self._cache:
            self._cache.move_to_end(key)
            self._hits += 1
            return self._cache[key]
        self._misses += 1
        return None

    def put(self, sql: str, result: QueryResult, context: str | None = None) -> None:
        key = self._make_key(sql, context)
        self._cache[key] = result
        self._cache.move_to_end(key)
        while len(self._cache) > self._max_size:
            self._cache.popitem(last=False)

    def clear(self) -> None:
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    @property
    def history(self) -> list[str]:
        return list(self._cache.keys())

    @property
    def size(self) -> int:
        return len(self._cache)

    @property
    def hit_rate(self) -> float:
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0

    @staticmethod
    def _normalize(sql: str) -> str:
        return " ".join(sql.strip().lower().split())
