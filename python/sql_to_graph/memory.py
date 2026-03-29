"""Persistent memory store for the data analyst agent.

Stores query history, learned facts, and user preferences in a JSON file.
The agent manages this automatically; users can force-purge when needed.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Literal


@dataclass
class MemoryEntry:
    """A single memory entry."""

    id: str
    type: Literal["query", "fact", "preference"]
    content: str
    sql: str | None = None
    metadata: dict = field(default_factory=dict)
    created_at: str = ""
    last_used_at: str = ""

    def age_seconds(self) -> float:
        """Seconds since this entry was created."""
        try:
            created = datetime.fromisoformat(self.created_at)
            return (datetime.now(timezone.utc) - created).total_seconds()
        except (ValueError, TypeError):
            return 0.0

    def age_human(self) -> str:
        """Human-readable age (e.g., '2min ago', '1h ago')."""
        secs = self.age_seconds()
        if secs < 60:
            return f"{int(secs)}s ago"
        elif secs < 3600:
            return f"{int(secs / 60)}min ago"
        elif secs < 86400:
            return f"{int(secs / 3600)}h ago"
        else:
            return f"{int(secs / 86400)}d ago"


class AgentMemory:
    """JSON-file-backed persistent memory for the data analyst agent.

    Args:
        path: File path for JSON persistence. ``None`` for in-memory only.
        max_entries: Maximum entries to keep. Oldest are evicted when exceeded.

    Example::

        memory = AgentMemory("/tmp/agent_memory.json")
        memory.remember_query(
            sql="SELECT COUNT(*) FROM orders",
            intent="total order count",
            result_summary={"row_count": 1, "columns": ["count"]},
        )
        # Later...
        history = memory.recall_queries(limit=5)
        context = memory.get_context_for_prompt()
        # Force purge
        memory.purge()  # deletes all
    """

    def __init__(self, path: str | None = None, max_entries: int = 200):
        self._path = path
        self._max_entries = max_entries
        self._entries: list[MemoryEntry] = []

        if path and os.path.exists(path):
            self.load()

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _make_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def _add(self, entry: MemoryEntry) -> str:
        self._entries.append(entry)
        # Evict oldest if over limit
        while len(self._entries) > self._max_entries:
            self._entries.pop(0)
        self._auto_save()
        return entry.id

    def _auto_save(self) -> None:
        if self._path:
            self.save()

    # ─── Remember ─────────────────────────────────────────────────────

    def remember_query(
        self,
        sql: str,
        intent: str,
        result_summary: dict | None = None,
    ) -> str:
        """Store a query with its intent. Returns entry ID."""
        now = self._now()
        entry = MemoryEntry(
            id=self._make_id(),
            type="query",
            content=intent,
            sql=sql,
            metadata=result_summary or {},
            created_at=now,
            last_used_at=now,
        )
        return self._add(entry)

    def remember_fact(self, fact: str, source_sql: str | None = None) -> str:
        """Store a learned fact about the data."""
        now = self._now()
        entry = MemoryEntry(
            id=self._make_id(),
            type="fact",
            content=fact,
            sql=source_sql,
            created_at=now,
            last_used_at=now,
        )
        return self._add(entry)

    def remember_preference(self, key: str, value: str) -> str:
        """Store a user preference. Updates existing pref with same key."""
        # Check for existing preference with same key
        for e in self._entries:
            if e.type == "preference" and e.metadata.get("key") == key:
                e.content = f"{key}: {value}"
                e.metadata["value"] = value
                e.last_used_at = self._now()
                self._auto_save()
                return e.id

        now = self._now()
        entry = MemoryEntry(
            id=self._make_id(),
            type="preference",
            content=f"{key}: {value}",
            metadata={"key": key, "value": value},
            created_at=now,
            last_used_at=now,
        )
        return self._add(entry)

    # ─── Recall ───────────────────────────────────────────────────────

    def recall(self, query: str, limit: int = 5) -> list[MemoryEntry]:
        """Fuzzy search memory by keyword matching."""
        query_lower = query.lower()
        keywords = query_lower.split()

        scored: list[tuple[int, MemoryEntry]] = []
        for entry in self._entries:
            searchable = f"{entry.content} {entry.sql or ''} {entry.type}".lower()
            score = sum(1 for kw in keywords if kw in searchable)
            if score > 0:
                scored.append((score, entry))

        # Highest score first, then most recently used, then newest created.
        scored.sort(
            key=lambda x: (x[0], x[1].last_used_at or x[1].created_at, x[1].created_at),
            reverse=True,
        )

        results = [entry for _, entry in scored[:limit]]
        # Update last_used_at
        now = self._now()
        for entry in results:
            entry.last_used_at = now
        if results:
            self._auto_save()
        return results

    def recall_queries(self, limit: int = 10) -> list[MemoryEntry]:
        """Get recent query history, most recent first."""
        queries = [e for e in self._entries if e.type == "query"]
        queries.sort(key=lambda e: e.created_at, reverse=True)
        return queries[:limit]

    def recall_facts(self) -> list[MemoryEntry]:
        """Get all stored facts."""
        return [e for e in self._entries if e.type == "fact"]

    def recall_preferences(self) -> list[MemoryEntry]:
        """Get all stored preferences."""
        return [e for e in self._entries if e.type == "preference"]

    # ─── Context for prompt ───────────────────────────────────────────

    def get_context_for_prompt(self, max_queries: int = 10) -> str:
        """Format relevant memories as text for system prompt injection."""
        lines: list[str] = []

        # Recent queries
        queries = self.recall_queries(limit=max_queries)
        if queries:
            lines.append("## Recent Query History\n")
            for i, q in enumerate(queries, 1):
                age = q.age_human()
                sql_preview = (q.sql or "")[:120].replace("\n", " ")
                lines.append(f"{i}. [{age}] Intent: \"{q.content}\" -> {sql_preview}")
            lines.append("")

        # Facts
        facts = self.recall_facts()
        if facts:
            lines.append("## Learned Facts\n")
            for f in facts:
                lines.append(f"- {f.content}")
            lines.append("")

        # Preferences
        prefs = self.recall_preferences()
        if prefs:
            lines.append("## User Preferences\n")
            for p in prefs:
                lines.append(f"- {p.content}")
            lines.append("")

        return "\n".join(lines)

    # ─── Purge ────────────────────────────────────────────────────────

    def purge(self, entry_id: str | None = None) -> int:
        """Purge a specific entry by ID, or ALL entries if id is None.

        Returns the number of entries deleted.
        """
        if entry_id is None:
            count = len(self._entries)
            self._entries.clear()
            self._auto_save()
            return count

        before = len(self._entries)
        self._entries = [e for e in self._entries if e.id != entry_id]
        deleted = before - len(self._entries)
        if deleted:
            self._auto_save()
        return deleted

    # ─── Persistence ──────────────────────────────────────────────────

    def save(self) -> None:
        """Persist to disk."""
        if not self._path:
            return
        parent = os.path.dirname(self._path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        data = {"entries": [asdict(e) for e in self._entries]}
        # Write atomically
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, self._path)

    def load(self) -> None:
        """Load from disk."""
        if not self._path or not os.path.exists(self._path):
            return
        with open(self._path, encoding="utf-8") as f:
            data = json.load(f)
        self._entries = [MemoryEntry(**e) for e in data.get("entries", [])]

    @property
    def size(self) -> int:
        return len(self._entries)
