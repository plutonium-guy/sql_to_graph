# sql-to-graph

`sql-to-graph` is a Python package with a Rust core for schema-aware SQL execution, query correction, statistics, chart generation, and agent-style data analysis.

It is designed for two common workflows:

1. You already have SQL and want fast execution, charting, export, and basic optimization.
2. You have a natural-language question and want an agent to discover schema, write SQL, retry on errors, summarize results, and choose a visualization.

The heavy lifting lives in Rust via PyO3 and `sqlx`; Python provides the public API, agent orchestration, caching, memory, and LLM integrations.

## Table of contents

- [Why this project exists](#why-this-project-exists)
- [Feature summary](#feature-summary)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Core API guide](#core-api-guide)
- [Data analyst agent](#data-analyst-agent)
  - [Quick start](#quick-start-1)
  - [LLM provider options](#llm-provider-options)
  - [Examples](#examples)
  - [AgentResponse reference](#agentresponse-reference)
  - [Memory and cache](#memory-and-cache)
  - [Planning and reflection](#planning-and-reflection)
  - [Event system](#event-system)
  - [Custom prompts](#custom-prompts)
  - [LangGraph integration](#langgraph-integration)
  - [Advanced patterns](#advanced-patterns)
- [LLM and tool integration](#llm-and-tool-integration)
- [Charts, statistics, and export](#charts-statistics-and-export)
- [Schema discovery and error recovery](#schema-discovery-and-error-recovery)
- [Architecture](#architecture)
- [Development](#development)
- [Testing](#testing)
- [Release workflow](#release-workflow)
- [License](#license)

## Why this project exists

Most SQL tooling does one of these things well:

- Parse SQL.
- Execute SQL.
- Visualize SQL results.
- Let an LLM call a SQL tool.

`sql-to-graph` tries to make those pieces work together cleanly:

- Parse and normalize SQL with dialect awareness.
- Use live schema metadata to improve correction and error recovery.
- Execute against PostgreSQL, MySQL, or SQLite.
- Compute statistics and suggest appropriate charts from result shape.
- Render HTML, SVG, PNG, or JPG charts.
- Expose the whole stack to Python agents and tool-calling LLMs.

## Feature summary

| Capability | What it does |
| --- | --- |
| SQL parsing | Validates SQL and returns parse errors plus normalized SQL |
| Query correction | Builds schema-aware correction prompts for an LLM |
| Query optimization | Applies AST-level rewrites before execution |
| Execution | Runs queries with read-only enforcement and enriched errors |
| Schema discovery | Lists schemas, describes tables, and samples rows |
| Statistics | Computes nulls, distinct counts, numeric summaries, and warnings |
| Chart suggestion | Ranks likely chart types based on result structure |
| Chart rendering | Produces HTML, PNG, JPG, or SVG |
| Export | Serializes results to CSV or JSON |
| Agent loop | Provides a ReAct-style data analyst agent with retries |
| Agent memory | Remembers prior queries, facts, and user preferences |
| Query cache | Reuses normalized SQL results across rounds |
| Planning | Decomposes complex questions into parallelizable sub-queries |
| Reflection | Reviews generated answers and can trigger retries |
| Tool adapters | Exposes OpenAI, Anthropic, MCP, and LangChain-friendly tools |

## Installation

Base install:

```bash
pip install sql-to-graph
```

With direct Anthropic/OpenAI support:

```bash
pip install "sql-to-graph[llm]"
```

With LangChain helpers:

```bash
pip install "sql-to-graph[langchain]"
```

With development dependencies:

```bash
pip install "sql-to-graph[dev]"
```

Project metadata:

- Python: `>=3.10`
- Package name: `sql-to-graph`
- Native extension module: `sql_to_graph._native`

## Quick start

### One-call query to chart

```python
from sql_to_graph import ChartConfig, ChartType, OutputFormat, sql_to_chart


async def main():
    result, chart = await sql_to_chart(
        sql="""
            SELECT department, COUNT(*) AS employee_count
            FROM employees
            GROUP BY department
            ORDER BY employee_count DESC
        """,
        connection_string="postgresql://user:pass@localhost/mydb",
        chart_config=ChartConfig(
            chart_type=ChartType.Bar,
            x_column="department",
            y_column="employee_count",
            title="Employees by department",
            output_format=OutputFormat.Html,
        ),
    )

    print(result.columns)
    print(result.row_count)
    print(chart.mime_type if chart else None)
```

### Synchronous wrapper

```python
from sql_to_graph import sql_to_chart_sync

result, chart = sql_to_chart_sync(
    sql="SELECT 1 AS value",
    connection_string="sqlite:///tmp/example.db",
)
```

## Core API guide

### Main types

The package exports a compact set of native result/configuration types:

- `Connection`
- `QueryResult`
- `ChartConfig`
- `ChartOutput`
- `SqlDialect`
- `ChartType`
- `OutputFormat`
- `CorrectionContext`
- `ParseResult`
- `ColumnStats`
- `ResultSummary`
- `ChartSuggestion`
- `EnrichedError`

### Lower-level execution flow

If you want full control instead of `sql_to_chart()`, the low-level pieces are available directly:

```python
from sql_to_graph import (
    ChartConfig,
    ChartType,
    Connection,
    OutputFormat,
    optimize_query,
    parse_sql,
    render_chart,
    suggest_charts,
    summarize_result,
)


async def run_manual(connection_string: str):
    conn = Connection(connection_string, read_only=True, schema="public")
    await conn.connect()
    try:
        sql = "SELECT signup_date, COUNT(*) AS signups FROM users GROUP BY signup_date"

        parsed = parse_sql(sql, conn.dialect)
        if not parsed.is_valid:
            raise ValueError(parsed.errors)

        optimized = optimize_query(sql, conn.dialect)
        result = await conn.execute_with_context(optimized, "public")

        summary = summarize_result(result)
        suggestions = suggest_charts(result)

        chart = render_chart(
            result,
            ChartConfig(
                chart_type=ChartType.Line,
                x_column="signup_date",
                y_column="signups",
                title="Daily signups",
                output_format=OutputFormat.Svg,
            ),
        )

        return result, summary, suggestions, chart
    finally:
        await conn.close()
```

### Convenience functions

The high-level helpers are:

- `sql_to_chart(...)`
- `sql_to_chart_sync(...)`
- `parse_sql(...)`
- `build_correction_context(...)`
- `apply_correction(...)`
- `optimize_query(...)`
- `render_chart(...)`
- `summarize_result(...)`
- `suggest_charts(...)`
- `export_csv(...)`
- `export_json(...)`

### Database support

Supported backends:

- PostgreSQL
- MySQL
- SQLite

The native `Connection` object can also:

- `list_schemas()`
- `get_metadata(schema=None)`
- `describe_table(table, schema=None)`
- `sample_table(table, n=10, schema=None)`
- `execute(sql)`
- `execute_paginated(sql, limit=1000, offset=0)`
- `execute_with_context(sql, schema=None)`

## Data analyst agent

`DataAnalystAgent` is a ReAct (Reason + Act) agent for natural-language data analysis. It follows a think/act/observe loop: receive a question, decide which tool to call, execute the tool, observe the result, and repeat until it has enough information to answer.

On the first `chat()` call, the agent connects to your database, discovers all schemas and tables, and injects the live metadata into its system prompt. From that point on, the LLM has full context about your database structure and can write accurate SQL without guessing table or column names.

The agent has five built-in tools:

| Tool | Purpose |
| --- | --- |
| `sql_to_graph` | Execute SQL with auto-correction, optimization, statistics, and chart rendering |
| `sql_discover_schemas` | List all schemas and their table counts |
| `sql_describe_table` | Get column names, types, nullability, and row count for a table |
| `sql_sample_data` | Fetch sample rows from a table |
| `sql_recall_queries` | Search agent memory for previously executed queries |

### Quick start

Minimal setup with Anthropic (requires `pip install "sql-to-graph[llm]"`):

```python
import asyncio
from sql_to_graph import DataAnalystAgent, create_llm


async def main():
    llm = create_llm("anthropic", model="claude-sonnet-4-20250514")
    agent = DataAnalystAgent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=llm,
    )

    response = await agent.chat("What are the top 10 customers by total spend?")
    print(response.text)
    print("SQL:", response.sql_executed)
    print("Rounds:", response.rounds_used)


asyncio.run(main())
```

Same thing with OpenAI:

```python
import asyncio
from sql_to_graph import DataAnalystAgent, create_llm


async def main():
    llm = create_llm("openai", model="gpt-4o")
    agent = DataAnalystAgent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=llm,
    )

    response = await agent.chat("Show me monthly revenue trends")
    print(response.text)


asyncio.run(main())
```

### LLM provider options

The `create_llm` factory returns a `UnifiedLLM` protocol object. Three providers are supported:

**Anthropic (direct)**

```python
from sql_to_graph import create_llm

llm = create_llm("anthropic", model="claude-sonnet-4-20250514")
# Uses ANTHROPIC_API_KEY env var, or pass api_key="sk-..."
```

**OpenAI (direct)**

```python
from sql_to_graph import create_llm

llm = create_llm("openai", model="gpt-4o")
# Uses OPENAI_API_KEY env var, or pass api_key="sk-..."
```

**LangChain (wraps any LangChain chat model)**

```python
from langchain_anthropic import ChatAnthropic
from sql_to_graph import create_llm

llm = create_llm("langchain", llm=ChatAnthropic(model="claude-sonnet-4-20250514"))
```

```python
from langchain_openai import ChatOpenAI
from sql_to_graph import create_llm

llm = create_llm("langchain", llm=ChatOpenAI(model="gpt-4o"))
```

**Legacy path (still works, not recommended)**

If you already have a raw async client:

```python
from anthropic import AsyncAnthropic
from sql_to_graph import DataAnalystAgent

agent = DataAnalystAgent(
    connection_string="postgresql://user:pass@localhost/mydb",
    llm_client=AsyncAnthropic(),
    model="claude-sonnet-4-20250514",
    provider_type="anthropic",
)
```

### Examples

#### Example 1: Self-contained SQLite (no external database needed)

This example creates a SQLite database, seeds it, and runs the agent. Copy, paste, and run:

```python
import asyncio
import sqlite3
from sql_to_graph import DataAnalystAgent, create_llm


def seed_database(path: str):
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            units_sold INTEGER NOT NULL
        );
        INSERT INTO products (name, category, price, units_sold) VALUES
            ('Widget A', 'Widgets', 9.99, 150),
            ('Widget B', 'Widgets', 14.99, 89),
            ('Gadget X', 'Gadgets', 24.99, 210),
            ('Gadget Y', 'Gadgets', 19.99, 175),
            ('Gizmo Alpha', 'Gizmos', 49.99, 42),
            ('Gizmo Beta', 'Gizmos', 39.99, 67),
            ('Widget C', 'Widgets', 12.49, 133),
            ('Gadget Z', 'Gadgets', 29.99, 95);
    """)
    conn.close()


async def main():
    db_path = "/tmp/demo.db"
    seed_database(db_path)

    llm = create_llm("anthropic", model="claude-sonnet-4-20250514")
    agent = DataAnalystAgent(
        connection_string=f"sqlite:///{db_path}",
        llm=llm,
    )

    response = await agent.chat("Which category has the highest total revenue?")
    print(response.text)
    print("SQL:", response.sql_executed)


asyncio.run(main())
```

#### Example 2: Auto-chart and save to HTML

The agent automatically suggests and renders charts when appropriate. Chart data is returned as base64-encoded strings in `response.charts`:

```python
import asyncio
from sql_to_graph import DataAnalystAgent, create_llm


async def main():
    llm = create_llm("anthropic", model="claude-sonnet-4-20250514")
    agent = DataAnalystAgent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=llm,
        default_format="html",
    )

    response = await agent.chat(
        "Show employees by department as a bar chart"
    )
    print(response.text)

    # Save any generated charts
    for i, chart in enumerate(response.charts):
        import base64

        data = base64.b64decode(chart["data_base64"])
        filename = f"chart_{i}.html"
        with open(filename, "wb") as f:
            f.write(data)
        print(f"Saved {filename} ({chart['mime_type']})")


asyncio.run(main())
```

#### Example 3: Multi-turn conversation

The agent maintains conversation history across `chat()` calls. The second question can reference prior context:

```python
import asyncio
from sql_to_graph import DataAnalystAgent, create_llm


async def main():
    llm = create_llm("openai", model="gpt-4o")
    agent = DataAnalystAgent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=llm,
    )

    # First question
    r1 = await agent.chat("What is the total revenue by region?")
    print("Q1:", r1.text)

    # Follow-up that builds on the first
    r2 = await agent.chat("Now break that down by month for the top 3 regions")
    print("Q2:", r2.text)

    # The agent reuses or adapts the prior SQL
    print("SQL:", r2.sql_executed)

    # Reset history when starting a new topic
    agent.reset()


asyncio.run(main())
```

#### Example 4: Export charts as PNG

Use `default_format="png"` to get rasterized chart images:

```python
import asyncio
import base64
from sql_to_graph import DataAnalystAgent, create_llm


async def main():
    llm = create_llm("anthropic", model="claude-sonnet-4-20250514")
    agent = DataAnalystAgent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=llm,
        default_format="png",
    )

    response = await agent.chat("Show a pie chart of order status distribution")

    for i, chart in enumerate(response.charts):
        image_bytes = base64.b64decode(chart["data_base64"])
        path = f"chart_{i}.png"
        with open(path, "wb") as f:
            f.write(image_bytes)
        print(f"Saved {path} ({len(image_bytes)} bytes)")


asyncio.run(main())
```

#### Example 5: Real-time event streaming

Use the `on_event` callback to monitor tool calls and reasoning rounds as they happen:

```python
import asyncio
from sql_to_graph import (
    DataAnalystAgent,
    PlanEvent,
    ReflectionEvent,
    RoundEvent,
    ToolCallEvent,
    create_llm,
)


def on_event(event):
    if isinstance(event, ToolCallEvent):
        status = "OK" if not event.error else f"ERROR: {event.error}"
        print(
            f"  [round {event.round}] {event.tool_name} "
            f"({event.duration_ms:.0f}ms) {status}"
        )
    elif isinstance(event, RoundEvent):
        label = "FINAL" if event.is_final else f"{len(event.tool_calls)} tool calls"
        print(f"Round {event.round}: {label}")
    elif isinstance(event, PlanEvent):
        print(f"Plan: {event.step_count} steps, simple={event.is_simple}")
        print(f"  Reasoning: {event.reasoning}")
    elif isinstance(event, ReflectionEvent):
        verdict = "accepted" if event.accepted else "rejected"
        print(f"Reflection attempt {event.attempt}: {verdict}")
        if event.critique:
            print(f"  Critique: {event.critique}")


async def main():
    llm = create_llm("anthropic", model="claude-sonnet-4-20250514")
    agent = DataAnalystAgent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=llm,
        on_event=on_event,
    )

    response = await agent.chat("What are the busiest hours for orders?")
    print("\n" + response.text)


asyncio.run(main())
```

#### Example 6: Persistent memory and query cache

Memory lets the agent recall and reuse prior queries across sessions. Cache avoids re-executing identical SQL within a session:

```python
import asyncio
from sql_to_graph import AgentMemory, DataAnalystAgent, QueryCache, create_llm


async def main():
    memory = AgentMemory(
        path="/tmp/sql_to_graph_memory.json",
        max_entries=200,
    )
    cache = QueryCache(max_size=100)

    llm = create_llm("openai", model="gpt-4o")
    agent = DataAnalystAgent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=llm,
        memory=memory,
        cache=cache,
    )

    # First query -- executes SQL and auto-remembers it
    r1 = await agent.chat("How many orders per month this year?")
    print(r1.text)

    # Second query -- the agent can find and adapt the prior query
    r2 = await agent.chat("Same thing but only for the US region")
    print(r2.text)

    # Inspect memory
    print(f"Memory entries: {memory.size}")
    recent = memory.recall_queries(limit=3)
    for entry in recent:
        print(f"  - {entry.content} | SQL: {entry.sql[:60]}...")

    # Search memory by keyword
    matches = memory.recall("revenue", limit=5)
    for m in matches:
        print(f"  Found: {m.content}")

    # Inspect cache
    print(f"Cache size: {cache.size}, hit rate: {cache.hit_rate:.0%}")

    # Purge all memory when done
    agent.purge_memory()


asyncio.run(main())
```

### AgentResponse reference

Every `chat()` call returns an `AgentResponse`:

| Field | Type | Description |
| --- | --- | --- |
| `text` | `str` | Final natural-language answer |
| `rounds_used` | `int` | Number of LLM reasoning rounds |
| `charts` | `list[dict]` | Rendered charts with `data_base64` and `mime_type` keys |
| `statistics` | `dict \| None` | Per-column statistics from the last query |
| `sql_executed` | `str \| None` | Final SQL the agent ran |
| `tool_calls` | `list[ToolCallEvent]` | Detailed telemetry for every tool execution |
| `errors` | `list[dict]` | Structured errors collected during retries |

### Memory and cache

**AgentMemory** provides persistent, JSON-file-backed storage. It automatically remembers every successful query the agent executes (SQL, intent, and result summary). The agent can search its memory through the built-in `sql_recall_queries` tool, letting it reuse and adapt prior queries instead of writing SQL from scratch.

Memory stores three types of entries:

- **Queries**: auto-stored after every successful SQL execution
- **Facts**: learned observations about the data
- **Preferences**: user-specified defaults

```python
from sql_to_graph import AgentMemory

# File-backed (persists across sessions)
memory = AgentMemory(path="/tmp/memory.json", max_entries=200)

# In-memory only (no persistence)
memory = AgentMemory()
```

**QueryCache** is an LRU cache that avoids re-executing identical (normalized) SQL within a session. It is created automatically if not provided.

```python
from sql_to_graph import QueryCache

cache = QueryCache(max_size=100)
```

Use `agent.purge_memory()` to clear all entries, or `agent.purge_memory(entry_id="...")` for a single entry.

### Planning and reflection

For complex, multi-part questions, the planner decomposes the question into independent sub-queries, executes them in parallel via `chat_isolated()`, and synthesizes the results. Reflection reviews the final answer for correctness and can trigger retries with feedback.

```python
import asyncio
from sql_to_graph import DataAnalystAgent, create_llm


async def main():
    main_llm = create_llm("anthropic", model="claude-sonnet-4-20250514")
    fast_llm = create_llm("anthropic", model="claude-haiku-4-5-20251001")

    agent = DataAnalystAgent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=main_llm,
        use_planner=True,
        planner_llm=fast_llm,       # cheaper model for planning
        use_reflection=True,
        reflector_llm=fast_llm,     # cheaper model for reflection
        max_reflections=2,           # retry up to 2 times
    )

    # This complex question gets decomposed into parallel sub-queries
    response = await agent.chat(
        "Compare average order value by region for Q1 vs Q2, "
        "and show which regions improved the most"
    )
    print(response.text)
    print(f"Total rounds: {response.rounds_used}")


asyncio.run(main())
```

The planner uses a heuristic (`needs_planning()`) to decide whether a question is complex enough to decompose. Simple questions skip the overhead and go straight to the React loop.

### Event system

The agent emits four event types through the `on_event` callback:

**`ToolCallEvent`** -- emitted for every tool execution:

- `round` (int) -- which reasoning round
- `tool_name` (str) -- which tool was called
- `arguments` (dict) -- the arguments passed (connection_string excluded)
- `result` (dict) -- the tool result
- `error` (str | None) -- error message if the call failed
- `duration_ms` (float) -- execution time

**`RoundEvent`** -- emitted at the end of each reasoning round:

- `round` (int) -- round number
- `tool_calls` (list[ToolCallEvent]) -- tool calls made this round
- `llm_text` (str) -- any text the LLM produced this round
- `is_final` (bool) -- True if this is the final answer

**`PlanEvent`** -- emitted when the planner creates a query plan:

- `step_count` (int) -- number of planned steps
- `is_simple` (bool) -- True if the planner considers it a single-step question
- `reasoning` (str) -- the planner's explanation

**`ReflectionEvent`** -- emitted when the reflection agent reviews an answer:

- `attempt` (int) -- reflection attempt number
- `accepted` (bool) -- True if the answer passed review
- `critique` (str | None) -- feedback if rejected

### Custom prompts

Use `custom_prompt` to inject domain-specific rules. This text is appended to the system prompt under an "Additional Instructions" heading, after the schema DDL:

```python
agent = DataAnalystAgent(
    connection_string="postgresql://...",
    llm=create_llm("anthropic", model="claude-sonnet-4-20250514"),
    custom_prompt=(
        "Revenue is stored in cents -- always divide by 100 for display.\n"
        "Always filter to tenant_id = 42 unless the user asks otherwise.\n"
        "The 'deleted_at' column means soft-deleted -- exclude those rows by default."
    ),
)
```

### LangGraph integration

If you prefer the LangGraph framework, `create_langgraph_agent()` returns a compiled graph pre-configured with sql_to_graph tools and live schema context:

```python
import asyncio
from langchain_anthropic import ChatAnthropic
from sql_to_graph import create_langgraph_agent


async def main():
    llm = ChatAnthropic(model="claude-sonnet-4-20250514")

    agent = await create_langgraph_agent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=llm,
        default_format="html",
    )

    result = await agent.ainvoke(
        {"messages": [("user", "What are the top 5 products by revenue?")]}
    )

    # The last message contains the agent's answer
    print(result["messages"][-1].content)


asyncio.run(main())
```

For more control, use `get_langchain_tools()` to build your own LangGraph agent:

```python
from sql_to_graph import get_langchain_tools

tools = get_langchain_tools(
    connection_string="postgresql://user:pass@localhost/mydb",
    schema="public",
)
# Pass these tools to langgraph.prebuilt.create_react_agent or your own graph
```

Note: the LangGraph integration does not include memory, planner, or reflection. For those features, use `DataAnalystAgent` directly.

### Advanced patterns

**Isolated chat for parallel queries**

`chat_isolated()` runs with its own message history, making it safe for concurrent calls. Schema, tools, cache, and memory are shared:

```python
import asyncio
from sql_to_graph import DataAnalystAgent, create_llm


async def main():
    llm = create_llm("anthropic", model="claude-sonnet-4-20250514")
    agent = DataAnalystAgent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=llm,
    )

    # Bootstrap schema once
    await agent.chat("How many tables are there?")
    agent.reset()

    # Run three questions in parallel
    results = await asyncio.gather(
        agent.chat_isolated("Total revenue this quarter"),
        agent.chat_isolated("Top 5 customers by order count"),
        agent.chat_isolated("Average order value by region"),
    )

    for r in results:
        print(f"({r.rounds_used} rounds) {r.text[:100]}...")


asyncio.run(main())
```

**TOONS encoding for token savings**

TOONS (Token-Optimized Object Notation for SQL) is enabled by default. It compresses tool results by 60-75% using a compact pipe-delimited format, reducing LLM token costs:

```python
from sql_to_graph import DataAnalystAgent, ToonsConfig, create_llm

agent = DataAnalystAgent(
    connection_string="postgresql://...",
    llm=create_llm("anthropic", model="claude-sonnet-4-20250514"),
    use_toons=True,                  # default
    toons_config=ToonsConfig(),      # default config
)

# To disable TOONS and send raw JSON to the LLM:
agent_raw = DataAnalystAgent(
    connection_string="postgresql://...",
    llm=create_llm("anthropic", model="claude-sonnet-4-20250514"),
    use_toons=False,
)
```

**Resetting conversation state**

```python
agent.reset()  # clears conversation history, keeps schema cache and memory
```

## LLM and tool integration

### OpenAI / Anthropic / MCP tools

The package can expose its tools in several formats:

```python
from sql_to_graph import (
    as_anthropic_tool,
    as_anthropic_tools,
    as_mcp_tools,
    as_openai_tool,
    as_openai_tools,
)
```

The common execution entry points are:

- `handle_tool_call(...)`
- `handle_discovery_call(...)`

Example:

```python
from sql_to_graph import handle_tool_call

result = await handle_tool_call(
    {
        "sql": "SELECT COUNT(*) AS cnt FROM orders",
        "connection_string": "postgresql://user:pass@localhost/mydb",
        "schema": "public",
        "include_stats": True,
        "suggest_charts": True,
        "optimize": True,
        "auto_correct": False,
    }
)
```

### LangChain tools

For LangChain integration:

```python
from sql_to_graph import get_langchain_tools

tools = get_langchain_tools(
    connection_string="postgresql://user:pass@localhost/mydb",
    schema="public",
)
```

## Charts, statistics, and export

### Chart types

Supported chart types:

- `Bar`
- `HorizontalBar`
- `StackedBar`
- `Line`
- `Area`
- `Pie`
- `Donut`
- `Scatter`
- `Histogram`
- `Heatmap`

Supported output formats:

- `Html`
- `Png`
- `Jpg`
- `Svg`

### Statistics

`summarize_result(...)` computes:

- null counts,
- distinct counts,
- min / max / mean / median / stddev for numeric columns,
- top categorical values,
- warnings for suspicious data quality patterns.

### Suggestions

`suggest_charts(...)` returns ranked `ChartSuggestion` objects containing:

- chart type,
- chosen columns,
- confidence score,
- reasoning text.

### Export

```python
from sql_to_graph import export_csv, export_json

csv_bytes = export_csv(result)
json_text = export_json(result)
```

## Schema discovery and error recovery

### Schema discovery

Schema discovery is important to both the core library and the agent.

Useful calls:

- `Connection.list_schemas()`
- `Connection.get_metadata(schema)`
- `Connection.describe_table(table, schema)`
- `Connection.sample_table(table, n, schema)`
- `build_schema_ddl(connection_string)`

### Error recovery

`execute_with_context(...)` enriches execution failures with:

- `error_type`
- `message`
- `original_sql`
- `available_tables`
- `available_columns`
- `suggestions`
- `schema_context`

This is especially useful for agent retries and for building human-friendly feedback loops.

## Architecture

### End-to-end flow

```mermaid
flowchart TD
    A["User question or raw SQL"] --> B["Python API"]
    B --> C["Rust core"]
    C --> D["sqlparser + optimizer"]
    C --> E["sqlx execution"]
    E --> F["QueryResult"]
    F --> G["Statistics + chart suggestions"]
    F --> H["Chart renderer"]
    B --> I["Agent orchestration"]
    I --> J["Tool adapters"]
    I --> K["Memory + cache + planner + reflection"]
```

### Module overview

| Module | Purpose |
| --- | --- |
| `src/executor.rs` | Database connection, execution, pagination, read-only checks, enriched errors |
| `src/parser.rs` | SQL parsing, correction prompt context, correction application |
| `src/optimizer.rs` | Query optimization rewrites |
| `src/chart.rs` | Chart generation entry point |
| `src/stats.rs` | Result statistics and warnings |
| `src/suggest.rs` | Chart recommendation logic |
| `src/metadata.rs` | Schema and table metadata discovery |
| `python/sql_to_graph/pipeline.py` | `sql_to_chart()` and `sql_to_chart_sync()` |
| `python/sql_to_graph/agent.py` | Tool schemas and generic tool handlers |
| `python/sql_to_graph/react_agent.py` | Main `DataAnalystAgent` loop |
| `python/sql_to_graph/memory.py` | Persistent JSON-backed memory |
| `python/sql_to_graph/cache.py` | LRU query cache |
| `python/sql_to_graph/planner.py` | Planning and parallel execution |
| `python/sql_to_graph/reflector.py` | Reflection-based answer review |
| `python/sql_to_graph/llm_factory.py` | Unified LLM abstraction and adapters |

## Development

### Local setup

```bash
git clone git@github.com:plutonium-guy/sql_to_graph.git
cd sql_to_graph

python -m venv .venv
source .venv/bin/activate

pip install -U pip
pip install -e ".[dev]"
uv run maturin develop
```

### Useful commands

```bash
cargo fmt
cargo test
uv run pytest -q
uv build
```

## Testing

The automated tests use a real PostgreSQL instance seeded with synthetic data across multiple schemas.

### Start PostgreSQL for tests

```bash
docker run -d --name sql_to_graph_test_pg \
    -e POSTGRES_PASSWORD=testpassword \
    -e POSTGRES_DB=testdb \
    -p 15432:5432 \
    postgres:16
```

### Run tests

```bash
uv run pytest -q
```

The test suite seeds the database automatically from `tests/seed_pg.sql`.

### Seeded schemas

- `ecommerce`
- `hr`
- `analytics`

### Test connection override

```bash
export TEST_PG_CONNECTION="postgresql://postgres:testpassword@localhost:15432/testdb"
```

## Release workflow

The project is packaged with `maturin`.

Local release artifacts:

```bash
uv build
```

This creates:

- a source distribution in `dist/`
- platform-specific wheels in `dist/`

Repository release flow:

1. Bump the crate/package version in `Cargo.toml`.
2. Commit the change.
3. Push to `main`.
4. Create and push a version tag such as `v0.1.6`.
5. GitHub Actions builds wheels for Linux, musllinux, Windows, and macOS.
6. The tag workflow publishes with `uv publish` using the repository secret `UV_PUBLISH_TOKEN`.

If you prefer a local publish script, see `scripts/publish.sh`.

## License

MIT
