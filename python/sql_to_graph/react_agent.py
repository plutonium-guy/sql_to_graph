"""React-style data analyst agent using sql_to_graph tools."""

from __future__ import annotations

import copy
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

from sql_to_graph._native import Connection
from sql_to_graph.agent import (
    as_anthropic_tools,
    as_openai_tools,
    handle_discovery_call,
    handle_tool_call,
)
from sql_to_graph.cache import QueryCache
from sql_to_graph.llm import LLMProvider
from sql_to_graph.memory import AgentMemory

logger = logging.getLogger("sql_to_graph.react_agent")


# ─── Schema formatter ──────────────────────────────────────────────────────

MAX_SCHEMA_TABLES = 80


async def build_schema_ddl(
    connection_string: str,
    max_tables: int = MAX_SCHEMA_TABLES,
) -> str:
    """Discover database schemas and format as DDL for injection into a prompt."""
    conn = Connection(connection_string, read_only=True)
    await conn.connect()

    try:
        schemas = await conn.list_schemas()
        lines: list[str] = []

        for schema_info in schemas:
            schema_name = schema_info.name
            lines.append(
                f"-- Schema: {schema_name} ({schema_info.table_count} tables)"
            )
            lines.append("")

            metadata_list = await conn.get_metadata(schema_name)

            # Sort by row count descending so we keep the most important tables
            metadata_list.sort(
                key=lambda m: m.row_count_estimate or 0, reverse=True
            )

            if len(metadata_list) > max_tables:
                metadata_list = metadata_list[:max_tables]
                lines.append(
                    f"-- (showing top {max_tables} tables by row count)"
                )
                lines.append("")

            for table_meta in metadata_list:
                row_est = table_meta.row_count_estimate
                row_str = f"~{row_est:,} rows" if row_est else "unknown rows"
                lines.append(f"-- Table: {table_meta.table_name} ({row_str})")
                for col in table_meta.columns:
                    nullable = "NULL" if col.is_nullable else "NOT NULL"
                    lines.append(
                        f"--   {col.name:<24s} {col.data_type:<16s} {nullable}"
                    )
                lines.append("")

        return "\n".join(lines)

    finally:
        await conn.close()


# ─── System prompt ─────────────────────────────────────────────────────────

SYSTEM_PROMPT_TEMPLATE = """\
You are a data analyst agent with direct access to a live database.
You answer questions by writing SQL, examining results, computing
statistics, and generating visualizations.

## Database Schema

{schema_ddl}

## Available Tools

You have 4 tools:

1. **sql_to_graph** - Execute SQL with auto-correction and optimization.
   - Always set `include_stats: true` to get column statistics.
   - Always set `suggest_charts: true` to get ranked chart suggestions.
   - When you know the right chart, include a `chart` parameter to render it.

2. **sql_discover_schemas** - List schemas (already done at startup,
   use only if the user asks about a different database).

3. **sql_describe_table** - Get column details for a specific table
   (use if you need to inspect a table not in the schema above).

4. **sql_sample_data** - Get sample rows to understand data values
   before writing a query.

## How to Decide on Visualization

After executing SQL with `suggest_charts: true`, the tool returns
ranked `chart_suggestions` with confidence scores and reasoning.
Follow this decision process:

1. If the top suggestion has confidence >= 0.85, use it directly.
2. If the top suggestion has confidence 0.6-0.85, consider the
   user's intent -- if they asked for a specific chart type, prefer
   that; otherwise use the top suggestion.
3. If all suggestions have confidence < 0.6 or there are no
   suggestions, present the data as a table and explain why no
   chart is appropriate.
4. If the user explicitly requested a chart type (e.g., "show me a
   pie chart"), honor that regardless of suggestions.

## How to Decide on Statistics

When `include_stats: true` is set, the response includes per-column
statistics. Highlight statistics that are:
- Surprising: high null percentage (>30%), zero variance, single
  distinct value
- Relevant to the user's question (e.g., averages for trends,
  distributions for outliers)
- Contained in the `warnings` array (always mention these)

## Output Format Rules

- Default output format: {default_format}
- Use `format: "html"` for interactive sessions (chat, notebook, web UI).
- Use `format: "png"` when the user asks to "download", "save",
  "export", or "attach" a chart, or when embedding in a document.
- Use `format: "svg"` when the user asks for a vector/scalable image.
- Use `format: "jpg"` only if explicitly requested.

## SQL Error Recovery

If a SQL query fails, the error response will include:
- `available_tables`: tables that exist in the database
- `suggestions`: fuzzy-matched corrections for misspelled names
- `schema_context`: the relevant schema for the failed query

Use these hints to fix the SQL and retry. Do NOT repeat the exact
same query that failed.

## Query Repurposing

Before writing a new query, check if a similar query exists in the
recent query history below (or use the sql_recall_queries tool).
If a prior query can be adapted:

1. For subset requests ("now just for region=East"): Add a WHERE clause
   to the prior query rather than rewriting from scratch.
2. For drill-downs ("break that down by month"): Wrap the prior query
   as a CTE and add GROUP BY.
3. For format changes ("show that as a pie chart"): Reuse the exact
   same SQL with a different chart config.

The query cache means reusing the same SQL is free (cached result).
Modified queries execute fresh but benefit from existing schema knowledge.

## Response Style

- Lead with a one-sentence answer to the user's question.
- Show the chart if one was generated.
- Highlight 2-3 key statistics.
- Show the SQL you ran (the optimized version from `sql_executed`).
- If data was paginated (`has_more: true`), mention that more rows
  exist and offer to paginate.
{memory_context}{custom_instructions}"""


# ─── Tool schema helpers ───────────────────────────────────────────────────

_DISCOVERY_TOOLS = {"sql_discover_schemas", "sql_describe_table", "sql_sample_data"}
_MEMORY_TOOL = "sql_recall_queries"

_RECALL_TOOL_ANTHROPIC = {
    "name": "sql_recall_queries",
    "description": (
        "Search past queries by keyword. Use this before writing SQL to "
        "check if a similar query was already run - you can modify it instead "
        "of starting from scratch."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Keywords to search for"},
            "limit": {"type": "integer", "default": 5, "description": "Max results"},
        },
        "required": ["query"],
    },
}

_RECALL_TOOL_OPENAI = {
    "type": "function",
    "function": {
        "name": "sql_recall_queries",
        "description": _RECALL_TOOL_ANTHROPIC["description"],
        "parameters": _RECALL_TOOL_ANTHROPIC["input_schema"],
    },
}


def _strip_connection_string(tools: list[dict]) -> list[dict]:
    """Remove connection_string from tool schemas (agent injects it)."""
    cleaned = []
    for tool in tools:
        tool = copy.deepcopy(tool)
        schema = tool.get("input_schema") or tool.get("function", {}).get("parameters")
        if schema and "properties" in schema:
            schema["properties"].pop("connection_string", None)
            if "required" in schema and "connection_string" in schema["required"]:
                schema["required"] = [
                    r for r in schema["required"] if r != "connection_string"
                ]
        cleaned.append(tool)
    return cleaned


# ─── Event types for tracing ──────────────────────────────────────────────

@dataclass
class ToolCallEvent:
    """Emitted for every tool call the agent makes."""

    round: int
    tool_name: str
    arguments: dict[str, Any]
    result: dict[str, Any]
    error: str | None
    duration_ms: float


@dataclass
class RoundEvent:
    """Emitted at the end of each agent reasoning round."""

    round: int
    tool_calls: list[ToolCallEvent]
    llm_text: str
    is_final: bool


# Type alias for the callback
OnEvent = Callable[[RoundEvent | ToolCallEvent], None]


# ─── Agent response ────────────────────────────────────────────────────────

@dataclass
class AgentResponse:
    """Structured response from the data analyst agent."""

    text: str
    rounds_used: int = 0
    charts: list[dict] = field(default_factory=list)
    statistics: dict | None = None
    sql_executed: str | None = None
    tool_calls: list[ToolCallEvent] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)


# ─── DataAnalystAgent ──────────────────────────────────────────────────────

class DataAnalystAgent:
    """React-style agent for data analysis using sql_to_graph tools.

    Args:
        connection_string: Database connection URL.
        llm_client: An Anthropic or OpenAI async client instance.
        model: Model name (e.g. "claude-sonnet-4-20250514", "gpt-4o").
        provider_type: "anthropic" or "openai".
        correction_llm: Optional LLMProvider for SQL auto-correction.
        cache: Optional QueryCache for caching query results.
        default_format: Default chart output format ("html", "png", "svg", "jpg").
        max_tool_rounds: Maximum number of LLM reasoning rounds (default 10).
        max_schema_tables: Max tables to include in the schema DDL.
        custom_prompt: Additional instructions appended to the system prompt.
            Use this to inject domain-specific context, business rules, or
            constraints (e.g. "always filter by tenant_id", "revenue is in cents").
        on_event: Callback invoked for every ToolCallEvent and RoundEvent.
            Use this to log, trace, or display decisions in real time.

    Example (Anthropic)::

        from anthropic import AsyncAnthropic
        from sql_to_graph.react_agent import DataAnalystAgent

        agent = DataAnalystAgent(
            connection_string="postgresql://user:pass@localhost/db",
            llm_client=AsyncAnthropic(),
            model="claude-sonnet-4-20250514",
            provider_type="anthropic",
            custom_prompt="Revenue values are stored in cents. Always divide by 100 for display.",
        )
        response = await agent.chat("What are the top 10 customers by revenue?")
        print(response.text)
        print(f"Completed in {response.rounds_used} rounds")

    Example (OpenAI)::

        from openai import AsyncOpenAI
        from sql_to_graph.react_agent import DataAnalystAgent

        agent = DataAnalystAgent(
            connection_string="postgresql://user:pass@localhost/db",
            llm_client=AsyncOpenAI(),
            model="gpt-4o",
            provider_type="openai",
        )
        response = await agent.chat("Show monthly sales trends")

    Example (observability)::

        def on_event(event):
            if isinstance(event, ToolCallEvent):
                print(f"  Round {event.round}: {event.tool_name} "
                      f"({'ERROR' if event.error else 'OK'}) {event.duration_ms:.0f}ms")
            elif isinstance(event, RoundEvent):
                print(f"Round {event.round} done (final={event.is_final})")

        agent = DataAnalystAgent(..., on_event=on_event)
    """

    def __init__(
        self,
        connection_string: str,
        llm_client: Any,
        model: str,
        provider_type: Literal["anthropic", "openai"],
        correction_llm: LLMProvider | None = None,
        cache: QueryCache | None = None,
        default_format: str = "html",
        max_tool_rounds: int = 10,
        max_schema_tables: int = MAX_SCHEMA_TABLES,
        custom_prompt: str | None = None,
        on_event: OnEvent | None = None,
        memory: AgentMemory | None = None,
    ):
        self._connection_string = connection_string
        self._llm_client = llm_client
        self._model = model
        self._provider_type = provider_type
        self._correction_llm = correction_llm
        self._cache = cache or QueryCache()
        self._default_format = default_format
        self._max_tool_rounds = max_tool_rounds
        self._max_schema_tables = max_schema_tables
        self._custom_prompt = custom_prompt
        self._on_event = on_event
        self._memory = memory

        self._schema_ddl: str | None = None
        self._system_prompt: str | None = None
        self._tools: list[dict] | None = None
        self._history: list[dict] = []

    async def _bootstrap_schema(self) -> None:
        """Discover DB schema and build the system prompt. Called once on first chat."""
        self._schema_ddl = await build_schema_ddl(
            self._connection_string, max_tables=self._max_schema_tables
        )
        self._rebuild_system_prompt()

        if self._provider_type == "anthropic":
            raw_tools = as_anthropic_tools()
            raw_tools.append(_RECALL_TOOL_ANTHROPIC)
        else:
            raw_tools = as_openai_tools()
            raw_tools.append(_RECALL_TOOL_OPENAI)
        self._tools = _strip_connection_string(raw_tools)

    def _rebuild_system_prompt(self) -> None:
        """Rebuild the system prompt, refreshing memory context."""
        custom_block = ""
        if self._custom_prompt:
            custom_block = f"\n## Additional Instructions\n\n{self._custom_prompt}\n"

        memory_context = ""
        if self._memory:
            ctx = self._memory.get_context_for_prompt()
            if ctx.strip():
                memory_context = f"\n{ctx}\n"

        self._system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
            schema_ddl=self._schema_ddl or "",
            default_format=self._default_format,
            memory_context=memory_context,
            custom_instructions=custom_block,
        )

    def _emit(self, event: RoundEvent | ToolCallEvent) -> None:
        if self._on_event is not None:
            try:
                self._on_event(event)
            except Exception:
                logger.warning("on_event callback raised", exc_info=True)

    async def _dispatch_tool_call(
        self, tool_name: str, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        """Route a tool call, injecting connection_string. Returns error dict on failure."""
        # Handle memory recall tool locally (no DB needed)
        if tool_name == _MEMORY_TOOL:
            return self._handle_recall(arguments)

        arguments["connection_string"] = self._connection_string

        try:
            if tool_name in _DISCOVERY_TOOLS:
                return await handle_discovery_call(tool_name, arguments)
            else:
                result = await handle_tool_call(
                    arguments,
                    llm=self._correction_llm,
                    cache=self._cache,
                    include_chart_data=True,
                )
                # Auto-remember successful queries
                if self._memory and "error" not in result and result.get("sql_executed"):
                    self._memory.remember_query(
                        sql=result["sql_executed"],
                        intent=arguments.get("sql", ""),
                        result_summary={
                            "row_count": result.get("row_count"),
                            "columns": result.get("columns"),
                        },
                    )
                return result
        except Exception as exc:
            error_msg = str(exc)
            enriched = _try_parse_enriched_error(error_msg)
            if enriched:
                return {"error": enriched}
            return {"error": {"message": error_msg}}

    def _handle_recall(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Handle the sql_recall_queries tool call."""
        if not self._memory:
            return {"queries": [], "message": "No memory configured."}

        query = arguments.get("query", "")
        limit = arguments.get("limit", 5)
        entries = self._memory.recall(query, limit=limit)
        return {
            "queries": [
                {
                    "intent": e.content,
                    "sql": e.sql,
                    "age": e.age_human(),
                    "result_summary": e.metadata,
                }
                for e in entries
            ]
        }

    # ─── Shared tool dispatch + tracking ──────────────────────────────

    async def _dispatch_and_track(
        self,
        round_num: int,
        tool_name: str,
        arguments: dict[str, Any],
        all_tool_calls: list[ToolCallEvent],
        charts: list[dict],
        errors: list[dict],
    ) -> tuple[dict[str, Any], str | None, dict | None]:
        """Dispatch a tool call, track it, emit events. Returns (result, sql, stats)."""
        t0 = time.monotonic()
        result = await self._dispatch_tool_call(tool_name, arguments)
        duration_ms = (time.monotonic() - t0) * 1000

        error_str = None
        if "error" in result:
            error_str = json.dumps(result["error"], default=str)
            errors.append({"round": round_num, "tool": tool_name, "error": result["error"]})

        event = ToolCallEvent(
            round=round_num,
            tool_name=tool_name,
            arguments={k: v for k, v in arguments.items() if k != "connection_string"},
            result=result,
            error=error_str,
            duration_ms=duration_ms,
        )
        all_tool_calls.append(event)
        self._emit(event)

        if "chart" in result and "data_base64" in result.get("chart", {}):
            charts.append(result["chart"])

        last_sql = result.get("sql_executed")
        last_stats = result.get("statistics")
        return result, last_sql, last_stats

    # ─── Anthropic tool-use loop ───────────────────────────────────────

    async def _chat_anthropic(self, user_message: str) -> AgentResponse:
        self._history.append({"role": "user", "content": user_message})
        messages = list(self._history)

        all_tool_calls: list[ToolCallEvent] = []
        charts: list[dict] = []
        errors: list[dict] = []
        last_stats: dict | None = None
        last_sql: str | None = None

        for round_num in range(1, self._max_tool_rounds + 1):
            response = await self._llm_client.messages.create(
                model=self._model,
                system=self._system_prompt,
                messages=messages,
                tools=self._tools,
                max_tokens=4096,
            )

            text_parts: list[str] = []
            tool_uses: list[dict] = []

            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_uses.append(
                        {"id": block.id, "name": block.name, "input": block.input}
                    )

            messages.append({"role": "assistant", "content": response.content})

            is_final = not tool_uses

            if is_final:
                final_text = "\n".join(text_parts)
                self._emit(RoundEvent(
                    round=round_num, tool_calls=[], llm_text=final_text, is_final=True,
                ))
                self._history.append({"role": "assistant", "content": final_text})
                return AgentResponse(
                    text=final_text,
                    rounds_used=round_num,
                    charts=charts,
                    statistics=last_stats,
                    sql_executed=last_sql,
                    tool_calls=all_tool_calls,
                    errors=errors,
                )

            # Dispatch tool calls
            round_events: list[ToolCallEvent] = []
            tool_results = []
            for tu in tool_uses:
                result, sql, stats = await self._dispatch_and_track(
                    round_num, tu["name"], tu["input"],
                    all_tool_calls, charts, errors,
                )
                round_events.append(all_tool_calls[-1])
                if sql:
                    last_sql = sql
                if stats:
                    last_stats = stats

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu["id"],
                    "content": json.dumps(result, default=str),
                })

            messages.append({"role": "user", "content": tool_results})
            self._emit(RoundEvent(
                round=round_num,
                tool_calls=round_events,
                llm_text="\n".join(text_parts),
                is_final=False,
            ))

        self._history.append(
            {"role": "assistant", "content": "Reached maximum tool call rounds."}
        )
        return AgentResponse(
            text="Reached maximum tool call rounds without completing analysis.",
            rounds_used=self._max_tool_rounds,
            charts=charts,
            statistics=last_stats,
            sql_executed=last_sql,
            tool_calls=all_tool_calls,
            errors=errors,
        )

    # ─── OpenAI tool-use loop ─────────────────────────────────────────

    async def _chat_openai(self, user_message: str) -> AgentResponse:
        self._history.append({"role": "user", "content": user_message})
        messages = [
            {"role": "system", "content": self._system_prompt},
            *self._history,
        ]

        all_tool_calls: list[ToolCallEvent] = []
        charts: list[dict] = []
        errors: list[dict] = []
        last_stats: dict | None = None
        last_sql: str | None = None

        for round_num in range(1, self._max_tool_rounds + 1):
            response = await self._llm_client.chat.completions.create(
                model=self._model,
                messages=messages,
                tools=self._tools,
                temperature=0,
            )

            choice = response.choices[0]
            message = choice.message
            messages.append(message.model_dump())

            is_final = not message.tool_calls

            if is_final:
                final_text = message.content or ""
                self._emit(RoundEvent(
                    round=round_num, tool_calls=[], llm_text=final_text, is_final=True,
                ))
                self._history.append({"role": "assistant", "content": final_text})
                return AgentResponse(
                    text=final_text,
                    rounds_used=round_num,
                    charts=charts,
                    statistics=last_stats,
                    sql_executed=last_sql,
                    tool_calls=all_tool_calls,
                    errors=errors,
                )

            round_events: list[ToolCallEvent] = []
            for tc in message.tool_calls:
                arguments = json.loads(tc.function.arguments)
                result, sql, stats = await self._dispatch_and_track(
                    round_num, tc.function.name, arguments,
                    all_tool_calls, charts, errors,
                )
                round_events.append(all_tool_calls[-1])
                if sql:
                    last_sql = sql
                if stats:
                    last_stats = stats

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(result, default=str),
                })

            self._emit(RoundEvent(
                round=round_num,
                tool_calls=round_events,
                llm_text=message.content or "",
                is_final=False,
            ))

        self._history.append(
            {"role": "assistant", "content": "Reached maximum tool call rounds."}
        )
        return AgentResponse(
            text="Reached maximum tool call rounds without completing analysis.",
            rounds_used=self._max_tool_rounds,
            charts=charts,
            statistics=last_stats,
            sql_executed=last_sql,
            tool_calls=all_tool_calls,
            errors=errors,
        )

    # ─── Public API ───────────────────────────────────────────────────

    async def chat(self, user_message: str) -> AgentResponse:
        """Send a message and get a structured response.

        On the first call, this bootstraps the DB schema into the system prompt.
        Subsequent calls maintain conversation history with refreshed memory.
        """
        if self._schema_ddl is None:
            await self._bootstrap_schema()
        else:
            # Refresh memory context in system prompt before each chat
            self._rebuild_system_prompt()

        if self._provider_type == "anthropic":
            return await self._chat_anthropic(user_message)
        else:
            return await self._chat_openai(user_message)

    def purge_memory(self, entry_id: str | None = None) -> int:
        """Purge agent memory. Pass entry_id for a single entry, None for all.

        Returns the number of entries deleted.
        """
        if not self._memory:
            return 0
        return self._memory.purge(entry_id)

    def reset(self) -> None:
        """Clear conversation history (keeps schema cache and memory)."""
        self._history.clear()


# ─── LangChain / LangGraph integration ───────────────────────────────────

async def create_langgraph_agent(
    connection_string: str,
    llm: Any,
    correction_llm: LLMProvider | None = None,
    cache: QueryCache | None = None,
    custom_prompt: str | None = None,
    default_format: str = "html",
    max_schema_tables: int = MAX_SCHEMA_TABLES,
) -> Any:
    """Create a LangGraph React agent pre-configured with sql_to_graph tools.

    Args:
        connection_string: Database connection URL.
        llm: A LangChain chat model (ChatAnthropic, ChatOpenAI, etc.).
        correction_llm: Optional LLMProvider for SQL auto-correction.
        cache: Optional QueryCache.
        custom_prompt: Additional instructions to append to the system prompt.
        default_format: Default chart format.
        max_schema_tables: Max tables in schema DDL.

    Returns:
        A LangGraph CompiledGraph agent ready for `.ainvoke()`.

    Example::

        from langchain_anthropic import ChatAnthropic
        from sql_to_graph.react_agent import create_langgraph_agent

        agent = await create_langgraph_agent(
            connection_string="postgresql://user:pass@localhost/db",
            llm=ChatAnthropic(model="claude-sonnet-4-20250514"),
            custom_prompt="All monetary values are in cents.",
        )
        result = await agent.ainvoke({
            "messages": [("user", "Show me monthly revenue trends")]
        })
        print(result["messages"][-1].content)
    """
    try:
        from langgraph.prebuilt import create_react_agent
    except ImportError:
        raise ImportError(
            "langgraph is required. Install with: pip install langgraph"
        )
    from sql_to_graph.langchain_tools import get_langchain_tools

    tools = get_langchain_tools(
        connection_string=connection_string,
        llm=correction_llm,
        cache=cache,
    )

    schema_ddl = await build_schema_ddl(connection_string, max_tables=max_schema_tables)

    custom_block = ""
    if custom_prompt:
        custom_block = f"\n## Additional Instructions\n\n{custom_prompt}\n"

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        schema_ddl=schema_ddl,
        default_format=default_format,
        memory_context="",
        custom_instructions=custom_block,
    )

    return create_react_agent(llm, tools, prompt=system_prompt)


# ─── Helpers ──────────────────────────────────────────────────────────────

def _try_parse_enriched_error(error_msg: str) -> dict | None:
    """Try to extract the enriched error JSON from an exception message."""
    try:
        parsed = json.loads(error_msg)
        if isinstance(parsed, dict) and "error_type" in parsed:
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    return None
