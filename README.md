# sql-to-graph

Rust-powered SQL auto-correction, optimization, execution, and chart generation. Built for AI agents.

```
pip install sql-to-graph
```

## What it does

1. **Auto-corrects** SQL queries using an LLM + actual database schema
2. **Optimizes** queries via AST rewrites (constant folding, boolean simplification, LIMIT pushdown)
3. **Executes** on PostgreSQL, MySQL, or SQLite with enriched error context
4. **Generates charts** from query results (HTML, PNG, JPG, SVG)
5. **Suggests charts** automatically based on column types and data shape
6. **Computes statistics** per column (mean, median, stddev, nulls, distinct count)
7. **Provides a full React agent** that reasons over data, writes SQL, picks visualizations, and self-corrects

Heavy lifting runs in Rust via PyO3. Python gets a clean async API.

## Table of contents

- [Quick start](#quick-start)
- [Data analyst agent](#data-analyst-agent)
  - [Anthropic](#anthropic)
  - [OpenAI](#openai)
  - [Observability](#observability)
  - [Custom prompt injection](#custom-prompt-injection)
  - [Agent memory](#agent-memory)
  - [Query caching](#query-caching)
  - [Query repurposing](#query-repurposing)
  - [LangGraph agent](#langgraph-agent)
- [Chart types](#chart-types)
- [LLM auto-correction](#llm-auto-correction)
- [Agent tool integration](#agent-tool-integration)
- [Schema discovery](#schema-discovery)
- [Statistics and chart suggestions](#statistics-and-chart-suggestions)
- [Pagination and export](#pagination-and-export)
- [SQL error recovery](#sql-error-recovery)
- [Databases](#databases)
- [Architecture](#architecture)
- [Testing](#testing)
- [License](#license)

## Quick start

```python
from sql_to_graph import sql_to_chart, ChartConfig, ChartType, OutputFormat

result, chart = await sql_to_chart(
    sql="SELECT department, COUNT(*) as cnt FROM employees GROUP BY department",
    connection_string="postgresql://user:pass@localhost/mydb",
    chart_config=ChartConfig(
        chart_type=ChartType.Bar,
        x_column="department",
        y_column="cnt",
        title="Employees by Department",
        output_format=OutputFormat.Html,
    ),
)

# result.columns, result.rows, result.row_count
# chart.data (bytes), chart.mime_type
```

Synchronous version:

```python
from sql_to_graph import sql_to_chart_sync

result, chart = sql_to_chart_sync(sql="...", connection_string="...")
```

## Data analyst agent

`DataAnalystAgent` is a React-style (Reason + Act) agent that connects to your database, discovers the schema, writes SQL, examines results, picks the best chart, and answers questions in natural language. It supports Anthropic and OpenAI as LLM backends.

### Anthropic

```python
from anthropic import AsyncAnthropic
from sql_to_graph import DataAnalystAgent

agent = DataAnalystAgent(
    connection_string="postgresql://user:pass@localhost/db",
    llm_client=AsyncAnthropic(),
    model="claude-sonnet-4-20250514",
    provider_type="anthropic",
)

response = await agent.chat("What are the top 10 customers by revenue?")
print(response.text)          # natural language answer
print(response.sql_executed)  # the SQL that was run
print(response.rounds_used)   # how many LLM rounds it took
print(response.charts)        # list of rendered chart dicts
print(response.statistics)    # column-level statistics
print(response.errors)        # any errors encountered (with recovery)
```

### OpenAI

```python
from openai import AsyncOpenAI
from sql_to_graph import DataAnalystAgent

agent = DataAnalystAgent(
    connection_string="postgresql://user:pass@localhost/db",
    llm_client=AsyncOpenAI(),
    model="gpt-4o",
    provider_type="openai",
)

response = await agent.chat("Show monthly sales trends as a line chart")
```

### Observability

Every tool call and reasoning round emits structured events via the `on_event` callback. Use this for logging, tracing, debugging, or building a live UI:

```python
from sql_to_graph import DataAnalystAgent, ToolCallEvent, RoundEvent

def on_event(event):
    if isinstance(event, ToolCallEvent):
        status = "ERROR" if event.error else "OK"
        print(f"  Round {event.round}: {event.tool_name} "
              f"({status}) {event.duration_ms:.0f}ms")
        if event.error:
            print(f"    Error: {event.error}")
    elif isinstance(event, RoundEvent):
        n_tools = len(event.tool_calls)
        print(f"Round {event.round} done — {n_tools} tool calls, "
              f"final={event.is_final}")

agent = DataAnalystAgent(
    connection_string="postgresql://...",
    llm_client=AsyncAnthropic(),
    model="claude-sonnet-4-20250514",
    provider_type="anthropic",
    on_event=on_event,
)
```

The `ToolCallEvent` contains:
- `round`: which reasoning round
- `tool_name`: which tool was called (`sql_to_graph`, `sql_describe_table`, etc.)
- `arguments`: the arguments passed (connection string is stripped for safety)
- `result`: the full result dict
- `error`: error string if the call failed, `None` otherwise
- `duration_ms`: wall-clock time for the call

The `RoundEvent` contains:
- `round`: the round number
- `tool_calls`: list of `ToolCallEvent` in this round
- `llm_text`: the LLM's text output for this round
- `is_final`: `True` if this is the last round (no more tool calls)

### Custom prompt injection

Inject domain-specific context, business rules, or constraints into the agent's system prompt:

```python
agent = DataAnalystAgent(
    connection_string="postgresql://...",
    llm_client=AsyncAnthropic(),
    model="claude-sonnet-4-20250514",
    provider_type="anthropic",
    custom_prompt=(
        "Revenue values are stored in cents. Always divide by 100 for display.\n"
        "Always filter by tenant_id = 42 unless the user specifies otherwise.\n"
        "The 'status' column uses codes: 1=active, 2=inactive, 3=suspended."
    ),
)
```

The custom prompt is appended as an `## Additional Instructions` section in the system prompt, after the schema DDL and tool descriptions.

### Agent memory

The agent supports persistent memory that survives across sessions. It automatically remembers successful queries and can store learned facts and user preferences:

```python
from sql_to_graph import DataAnalystAgent, AgentMemory

# Memory persists to a JSON file
memory = AgentMemory(path="/tmp/agent_memory.json", max_entries=200)

agent = DataAnalystAgent(
    connection_string="postgresql://...",
    llm_client=AsyncAnthropic(),
    model="claude-sonnet-4-20250514",
    provider_type="anthropic",
    memory=memory,
)

# Queries are automatically remembered after execution
await agent.chat("What's the monthly revenue trend?")

# Memory is searchable — the LLM can call sql_recall_queries to find past work
await agent.chat("Show me the same revenue data but just for the East region")

# Manually store facts and preferences
memory.remember_fact("Revenue is stored in cents", source_sql="SELECT revenue FROM orders")
memory.remember_preference("chart_format", "png")

# Search memory
results = memory.recall("revenue", limit=5)
for entry in results:
    print(f"[{entry.age_human()}] {entry.content}: {entry.sql}")

# Get formatted context (injected into the system prompt automatically)
print(memory.get_context_for_prompt())

# Purge memory
memory.purge()                    # delete all entries
memory.purge(entry_id="abc123")   # delete a specific entry

# Force purge via the agent
agent.purge_memory()              # delegates to memory.purge()
```

Memory stores three types of entries:

| Type | What it stores | How it's used |
|------|---------------|---------------|
| `query` | SQL + intent + result summary | Auto-stored after each successful query. The LLM sees recent queries in the system prompt and can call `sql_recall_queries` to search them. |
| `fact` | Learned insights about the data | Injected into the system prompt so the LLM remembers things like "revenue is in cents". |
| `preference` | User preferences (key-value) | Injected into the system prompt. Updating a preference with the same key overwrites the old value. |

Storage format is a single JSON file. Writes are atomic (write to `.tmp`, then `os.replace`). Oldest entries are evicted when `max_entries` is exceeded.

### Query caching

Results are cached by normalized SQL + connection string. Identical queries across rounds or conversations return instantly from cache:

```python
from sql_to_graph import DataAnalystAgent, QueryCache

cache = QueryCache(max_size=100)

agent = DataAnalystAgent(
    connection_string="postgresql://...",
    llm_client=AsyncAnthropic(),
    model="claude-sonnet-4-20250514",
    provider_type="anthropic",
    cache=cache,
)

# First call executes the query
await agent.chat("Count all orders")

# If the LLM generates the same SQL again, it's a cache hit (0ms)
await agent.chat("How many orders are there?")

# Check cache stats
print(f"Hit rate: {cache.hit_rate:.0%}")
print(f"Cache size: {cache.size}")
```

Cache features:
- **LRU eviction**: least recently used entries are evicted when `max_size` is exceeded
- **SQL normalization**: `SELECT  1` and `select 1` are the same cache key
- **Context isolation**: same SQL against different databases are separate cache entries (keyed by SHA-256 of the connection string)

### Query repurposing

When a user refines a question ("now show me just region=East"), the agent recognizes it can modify the prior query rather than writing from scratch. This works through:

1. **System prompt instructions**: the agent is told to check query history before writing new SQL
2. **`sql_recall_queries` tool**: the LLM can search past queries by keyword
3. **Memory context**: recent queries are shown in the system prompt with their intent and SQL

```python
# First query: full aggregation
await agent.chat("Show me revenue by region")
# → SELECT region, SUM(total_amount) FROM orders GROUP BY region

# Refinement: agent sees the prior query and adds a WHERE clause
await agent.chat("Now just for the East region")
# → SELECT region, SUM(total_amount) FROM orders WHERE region = 'East' GROUP BY region

# Drill-down: agent wraps prior query as a CTE
await agent.chat("Break that down by month")
# → WITH base AS (SELECT ...) SELECT DATE_TRUNC('month', ...) FROM base GROUP BY 1

# Format change: reuses exact same SQL, changes chart config (cache hit!)
await agent.chat("Show that as a pie chart instead")
```

### LangGraph agent

Create a pre-configured LangGraph React agent with all sql_to_graph tools:

```python
from langchain_anthropic import ChatAnthropic
from sql_to_graph import create_langgraph_agent

agent = await create_langgraph_agent(
    connection_string="postgresql://user:pass@localhost/db",
    llm=ChatAnthropic(model="claude-sonnet-4-20250514"),
    custom_prompt="All monetary values are in cents.",
)

result = await agent.ainvoke({
    "messages": [("user", "Show me monthly revenue trends")]
})
print(result["messages"][-1].content)
```

Install:

```bash
pip install 'sql-to-graph[langchain]'
pip install langgraph langchain-anthropic  # or langchain-openai
```

### Agent constructor reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `connection_string` | `str` | required | Database connection URL |
| `llm_client` | `AsyncAnthropic \| AsyncOpenAI` | required | LLM client instance |
| `model` | `str` | required | Model name (e.g. `"claude-sonnet-4-20250514"`, `"gpt-4o"`) |
| `provider_type` | `"anthropic" \| "openai"` | required | Which LLM provider |
| `correction_llm` | `LLMProvider \| None` | `None` | Separate LLM for SQL auto-correction |
| `cache` | `QueryCache \| None` | auto-created | Query result cache |
| `memory` | `AgentMemory \| None` | `None` | Persistent memory store |
| `default_format` | `str` | `"html"` | Default chart output format |
| `max_tool_rounds` | `int` | `10` | Max LLM reasoning rounds before stopping |
| `max_schema_tables` | `int` | `80` | Max tables to include in schema DDL |
| `custom_prompt` | `str \| None` | `None` | Additional instructions for the system prompt |
| `on_event` | `Callable \| None` | `None` | Callback for `ToolCallEvent` and `RoundEvent` |

### Agent response structure

```python
@dataclass
class AgentResponse:
    text: str                          # natural language answer
    rounds_used: int                   # number of LLM reasoning rounds
    charts: list[dict]                 # rendered charts (format, mime_type, data_base64)
    statistics: dict | None            # column-level statistics from the last query
    sql_executed: str | None           # the final SQL that was executed
    tool_calls: list[ToolCallEvent]    # all tool calls across all rounds
    errors: list[dict]                 # errors encountered (tool failures, SQL errors)
```

### Agent decision flow

The agent follows this multi-round loop:

```
User question
    │
    ▼
┌─────────────────────────────────────┐
│  Round 1: LLM reasons about the     │
│  question, checks query history,    │
│  decides what tools to call         │
│                                     │
│  Tools available:                   │
│  • sql_to_graph (execute SQL)       │
│  • sql_discover_schemas             │
│  • sql_describe_table               │
│  • sql_sample_data                  │
│  • sql_recall_queries (memory)      │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Tool execution                     │
│  • SQL auto-correction (if LLM)     │
│  • Query optimization (AST)         │
│  • Execute against database         │
│  • Cache result                     │
│  • Remember in memory               │
│  • Compute statistics               │
│  • Suggest chart types              │
│  • Render chart (if requested)      │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Round 2+: LLM examines results,   │
│  decides if more queries needed,    │
│  picks chart, or gives final answer │
└──────────────┬──────────────────────┘
               │
               ▼
         AgentResponse
```

Typical queries complete in **2-3 rounds**: one to execute SQL, one to formulate the answer. Complex analyses (error recovery, multi-step exploration) may take more. The agent stops at `max_tool_rounds` if it doesn't converge.

## Chart types

| Type | Enum | Best for |
|------|------|----------|
| Bar | `ChartType.Bar` | Categorical comparisons |
| Horizontal Bar | `ChartType.HorizontalBar` | Long category labels |
| Stacked Bar | `ChartType.StackedBar` | Part-of-whole over categories |
| Line | `ChartType.Line` | Temporal trends |
| Area | `ChartType.Area` | Cumulative trends |
| Pie | `ChartType.Pie` | Part-of-whole (2-8 categories) |
| Donut | `ChartType.Donut` | Same as pie, with center label |
| Scatter | `ChartType.Scatter` | Correlation between 2 numeric columns |
| Histogram | `ChartType.Histogram` | Distribution of a single numeric column |
| Heatmap | `ChartType.Heatmap` | 2 categorical + 1 numeric (e.g. A/B tests) |

Output formats: `OutputFormat.Html` (interactive), `OutputFormat.Png`, `OutputFormat.Jpg`, `OutputFormat.Svg` (vector)

```python
from sql_to_graph import render_chart, ChartConfig, ChartType, OutputFormat

chart_output = render_chart(
    result,
    ChartConfig(
        chart_type=ChartType.Scatter,
        x_column="quantity",
        y_column="unit_price",
        title="Order Items: Quantity vs Price",
        output_format=OutputFormat.Png,
    ),
)

with open("chart.png", "wb") as f:
    f.write(chart_output.data)
```

## LLM auto-correction

Pass any LLM provider to enable SQL auto-correction against the real database schema:

```python
from sql_to_graph import OpenAIProvider, AnthropicProvider, LangChainProvider

# OpenAI / OpenAI-compatible
llm = OpenAIProvider(model="gpt-4o")

# Anthropic
llm = AnthropicProvider(model="claude-sonnet-4-20250514")

# Any LangChain chat model
from langchain_openai import ChatOpenAI
llm = LangChainProvider(ChatOpenAI(model="gpt-4o"))

result, chart = await sql_to_chart(
    sql="SELECT * FORM users WERE age > 30",  # typos!
    connection_string="postgresql://...",
    llm=llm,  # auto-corrects using DB schema
)
```

Install LLM dependencies:

```bash
pip install 'sql-to-graph[llm]'        # openai + anthropic
pip install 'sql-to-graph[langchain]'   # langchain-core
```

### Custom LLM provider

Any object with an async `complete` method works:

```python
class MyProvider:
    async def complete(self, prompt: str, system: str | None = None) -> str:
        return await my_llm_call(prompt)
```

## Agent tool integration

Works with OpenAI, Anthropic, and MCP tool calling out of the box.

### Single tool

```python
from sql_to_graph import as_openai_tool, as_anthropic_tool

openai_tool = as_openai_tool()      # OpenAI function-calling format
anthropic_tool = as_anthropic_tool() # Anthropic tool-use format
```

### Full toolkit (query + schema discovery)

```python
from sql_to_graph import as_openai_tools, as_anthropic_tools, as_mcp_tools

tools = as_openai_tools()     # 4 tools: query, discover schemas, describe table, sample data
tools = as_anthropic_tools()  # same, Anthropic format
tools = as_mcp_tools()        # same, MCP format
```

### Handling tool calls

```python
from sql_to_graph import handle_tool_call, handle_discovery_call, QueryCache

cache = QueryCache(max_size=100)

# For sql_to_graph tool calls — returns a dict, never raises
response = await handle_tool_call(
    arguments=tool_call.arguments,
    llm=llm,           # optional: enables auto-correction
    cache=cache,        # optional: caches results
)
# response = {"sql_executed": "...", "columns": [...], "rows": [...], ...}
# or on error: {"error": {"error_type": "...", "message": "...", ...}, "sql_executed": "..."}

# For discovery tool calls (schemas, describe, sample)
response = await handle_discovery_call(
    tool_name="sql_describe_table",
    arguments={"connection_string": "...", "table": "users"},
)
```

### LangChain tools

Drop-in tools for any LangChain agent (ReAct, LangGraph, etc.):

```python
from sql_to_graph import get_langchain_tools

# Get all 4 tools: sql_query, sql_discover_schemas, sql_describe_table, sql_sample_data
tools = get_langchain_tools("postgresql://user:pass@localhost/db")

# With LLM auto-correction
from sql_to_graph import LangChainProvider
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(model="gpt-4o")
tools = get_langchain_tools(
    connection_string="postgresql://user:pass@localhost/db",
    llm=LangChainProvider(llm),
)

# Use with LangGraph ReAct agent
from langgraph.prebuilt import create_react_agent

agent = create_react_agent(llm, tools)
result = await agent.ainvoke({"messages": [("user", "Show me sales by region")]})

# Use with legacy AgentExecutor
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain_core.prompts import ChatPromptTemplate

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a data analyst. Use the SQL tools to answer questions."),
    ("human", "{input}"),
    ("placeholder", "{agent_scratchpad}"),
])
agent = create_openai_tools_agent(llm, tools, prompt)
executor = AgentExecutor(agent=agent, tools=tools)
result = await executor.ainvoke({"input": "What are the top 10 customers by revenue?"})
```

Install:

```bash
pip install 'sql-to-graph[langchain]'
```

## Schema discovery

```python
from sql_to_graph import Connection

conn = Connection("postgresql://...", read_only=True)
await conn.connect()

schemas = await conn.list_schemas()              # list of SchemaInfo
metadata = await conn.get_metadata("public")     # all tables in schema
table = await conn.describe_table("users")       # columns, types, row estimate
sample = await conn.sample_table("users", n=5)   # sample rows

await conn.close()
```

For the agent, `build_schema_ddl` formats the full database schema as DDL text for system prompt injection:

```python
from sql_to_graph import build_schema_ddl

ddl = await build_schema_ddl("postgresql://user:pass@localhost/db")
# Returns formatted DDL with schema names, table names, column types,
# nullability, and row count estimates
```

## Statistics and chart suggestions

```python
from sql_to_graph import summarize_result, suggest_charts

result = await conn.execute_with_context("SELECT * FROM sales", "public")

# Column statistics: min, max, mean, median, stddev, nulls, distinct count
summary = summarize_result(result)
for stat in summary.column_stats:
    print(f"{stat.column_name}: mean={stat.mean}, nulls={stat.null_count}, "
          f"distinct={stat.distinct_count}")

# Warnings for data quality issues (high nulls, zero variance, etc.)
for warning in summary.warnings:
    print(f"Warning: {warning}")

# Auto-suggest best chart types for the data
suggestions = suggest_charts(result)
for s in suggestions:
    print(f"{s.chart_type} - {s.title} (confidence: {s.confidence:.2f})")
    print(f"  x={s.x_column}, y={s.y_column}, reason: {s.reasoning}")
```

The suggestion engine applies 7 heuristic rules based on column types:

| Rule | Columns detected | Suggested chart |
|------|-----------------|----------------|
| 1 | Temporal + numeric | Line / Area |
| 2 | Categorical (2-8 values) + numeric | Bar / Pie / Donut |
| 3 | Temporal + 2+ numeric | Stacked Bar / multi-Line |
| 4 | 2 numeric columns | Scatter |
| 5 | 1 numeric column (>50 rows) | Histogram |
| 6 | Categorical (>8 values) + numeric | Horizontal Bar |
| 7 | 2 categorical + 1 numeric | Heatmap |

Each suggestion includes a confidence score (0-1) and reasoning text.

## Pagination and export

```python
# Paginated queries
result = await conn.execute_paginated("SELECT * FROM big_table", limit=100, offset=0)
print(f"Showing {result.row_count} of {result.total_row_count}, has_more={result.has_more}")

# Export
from sql_to_graph import export_csv, export_json

csv_bytes = export_csv(result)    # bytes
json_str = export_json(result)    # JSON string
```

## SQL error recovery

When a query fails, `execute_with_context` returns enriched error information that helps the agent self-correct:

```python
from sql_to_graph import Connection

conn = Connection("postgresql://...", read_only=True, schema="ecommerce")
await conn.connect()

try:
    # Misspelled table name
    result = await conn.execute_with_context(
        "SELECT * FROM ecommerce.cusotmers", "ecommerce"
    )
except Exception as exc:
    import json
    error = json.loads(str(exc))
    print(error)
```

The enriched error contains:

```json
{
  "error_type": "execution_error",
  "message": "relation \"ecommerce.cusotmers\" does not exist",
  "original_sql": "SELECT * FROM ecommerce.cusotmers",
  "available_tables": ["customers", "orders", "products", "order_items", "monthly_revenue"],
  "suggestions": ["customers"],
  "schema_context": "Table: customers\n  id (integer)\n  name (character varying)\n  ..."
}
```

- **`available_tables`**: all tables in the schema, so the agent knows what exists
- **`suggestions`**: fuzzy-matched table/column names (Levenshtein distance) for the misspelled identifier
- **`schema_context`**: full schema DDL for the relevant tables, so the agent can fix column references

When used through `handle_tool_call` or the `DataAnalystAgent`, errors are returned as dicts (never raised), so the agent loop can inspect them and retry with corrected SQL.

## Databases

| Database | Connection string | Notes |
|----------|-------------------|-------|
| PostgreSQL | `postgresql://user:pass@host:5432/dbname` | Full support including schemas |
| MySQL | `mysql://user:pass@host:3306/dbname` | |
| SQLite | `sqlite:///path/to/db.sqlite` | Single file, no schemas |

All connections default to `read_only=True` to prevent accidental writes.

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│                    Python API Layer                        │
│                                                           │
│  DataAnalystAgent    LangChain Tools    Pipeline (1-call) │
│  (react_agent.py)    (langchain_tools)  (pipeline.py)     │
│       │                    │                  │            │
│  AgentMemory         Agent Adapters     LLM Providers     │
│  (memory.py)         (agent.py)         (llm.py)          │
│       │                    │                  │            │
│  QueryCache                │                  │            │
│  (cache.py)                │                  │            │
└────────────────────────────┼──────────────────┼───────────┘
                             │                  │
                    ┌────────┴──────────────────┴───────────┐
                    │           Rust Core (PyO3)             │
                    │                                        │
                    │  Connection    SQL Parser/Optimizer     │
                    │  (sqlx)        (sqlparser-rs)           │
                    │                                        │
                    │  Chart Renderer    Statistics Engine    │
                    │  (plotters)        (summarize/suggest)  │
                    │                                        │
                    │  Error Enrichment  Export (CSV/JSON)    │
                    │  (fuzzy match)                          │
                    └────────────────────────────────────────┘
```

### Module overview

| Module | Purpose |
|--------|---------|
| `react_agent.py` | `DataAnalystAgent` — full React agent loop with Anthropic/OpenAI, schema bootstrapping, memory integration, tool dispatch |
| `agent.py` | Tool schema generation (`as_openai_tools`, `as_anthropic_tools`, `as_mcp_tools`) and universal `handle_tool_call`/`handle_discovery_call` handlers |
| `memory.py` | `AgentMemory` — JSON-file-backed persistent store for queries, facts, and preferences |
| `cache.py` | `QueryCache` — LRU cache with SQL normalization and connection-context isolation |
| `llm.py` | `LLMProvider` protocol + `OpenAIProvider`, `AnthropicProvider`, `LangChainProvider` implementations |
| `langchain_tools.py` | LangChain `BaseTool` wrappers for all 4 tools |
| `pipeline.py` | `sql_to_chart` / `sql_to_chart_sync` — one-call convenience functions |
| `_native` | Rust extension module (Connection, SQL parsing, optimization, chart rendering, statistics, suggestions, error enrichment, export) |

## Testing

The test suite runs against a real PostgreSQL database with synthetic data across 3 schemas (ecommerce, hr, analytics) totaling ~5000 rows.

### Setup

```bash
# Start PostgreSQL in Docker
docker run -d --name sql_to_graph_test_pg \
    -e POSTGRES_PASSWORD=testpassword \
    -e POSTGRES_DB=testdb \
    -p 15432:5432 \
    postgres:16

# Install dev dependencies
pip install 'sql-to-graph[dev]'

# Run tests
pytest tests/ -v
```

The test suite automatically seeds the database on first run using `tests/seed_pg.sql`.

### Test coverage

| Test file | Tests | What it covers |
|-----------|-------|----------------|
| `test_connection.py` | 6 | Schema discovery, metadata, cross-schema isolation, row count estimates |
| `test_query.py` | 6 | Execute, paginate, read-only enforcement, joins |
| `test_stats.py` | 6 | Numeric/categorical statistics, null warnings, single-value columns |
| `test_suggest.py` | 7 | Chart suggestion rules (line, bar, pie, scatter, histogram, heatmap) |
| `test_cache.py` | 7 | Hits, misses, normalization, LRU eviction, context isolation |
| `test_error_recovery.py` | 5 | Enriched errors, fuzzy suggestions, `handle_tool_call` error dicts |
| `test_memory.py` | 10 | Remember/recall, persistence, purge, eviction, prompt context |
| `test_react_agent.py` | 9 | Full agent loop with mock LLM, error retry, cache, memory, recall tool |

### Seed data schemas

**`ecommerce`** (5 tables): customers (200), products (50), orders (1000), order_items (2500), monthly_revenue (36)

**`hr`** (3 tables): departments (8), employees (500), performance_reviews (1000)

**`analytics`** (2 tables): page_views (730), ab_test_results (60)

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TEST_PG_CONNECTION` | `postgresql://postgres:testpassword@localhost:15432/testdb` | PostgreSQL connection string for tests |

## License

MIT
