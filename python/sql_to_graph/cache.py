"""Simple query cache to avoid re-executing identical queries."""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sql_to_graph._native import QueryResult


class QueryCache:
    """LRU cache for query results. Avoids re-executing identical SQL."""

    def __init__(self, max_size: int = 100):
        self._cache: OrderedDict[str, QueryResult] = OrderedDict()
        self._max_size = max_size

    def get(self, sql: str) -> QueryResult | None:
        key = self._normalize(sql)
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        return None

    def put(self, sql: str, result: QueryResult) -> None:
        key = self._normalize(sql)
        self._cache[key] = result
        self._cache.move_to_end(key)
        while len(self._cache) > self._max_size:
            self._cache.popitem(last=False)

    def clear(self) -> None:
        self._cache.clear()

    @property
    def history(self) -> list[str]:
        return list(self._cache.keys())

    @property
    def size(self) -> int:
        return len(self._cache)

    @staticmethod
    def _normalize(sql: str) -> str:
        return " ".join(sql.strip().lower().split())
