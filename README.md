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

`DataAnalystAgent` is the higher-level interface for natural-language data analysis.

It:

- discovers schema at startup,
- builds a system prompt from real metadata,
- exposes execution and discovery tools,
- lets the LLM iterate with tool calls,
- adds optional cache, memory, planning, reflection, and TOONS encoding,
- returns a structured `AgentResponse`.

### Recommended setup

The recommended path is the unified LLM factory:

```python
from sql_to_graph import DataAnalystAgent, create_llm


async def main():
    llm = create_llm("anthropic", model="claude-sonnet-4-20250514")

    agent = DataAnalystAgent(
        connection_string="postgresql://user:pass@localhost/mydb",
        llm=llm,
        default_format="html",
        use_planner=True,
        use_reflection=True,
    )

    response = await agent.chat("Show monthly revenue by region as a chart")
    print(response.text)
    print(response.sql_executed)
    print(response.rounds_used)
```

### Legacy client path

If you already have an async client instance, the older constructor style still works:

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

### Agent response

`AgentResponse` includes:

- `text`: final natural-language answer
- `rounds_used`: number of reasoning rounds
- `charts`: rendered charts returned from tool calls
- `statistics`: column statistics from the last relevant query
- `sql_executed`: final SQL the agent ran
- `tool_calls`: detailed per-call telemetry
- `errors`: structured errors collected during retries

### Observability

The agent emits structured events through `on_event`:

- `ToolCallEvent`
- `RoundEvent`
- `PlanEvent`
- `ReflectionEvent`

```python
from sql_to_graph import DataAnalystAgent, ToolCallEvent, RoundEvent


def on_event(event):
    if isinstance(event, ToolCallEvent):
        print(event.round, event.tool_name, event.duration_ms, event.error)
    elif isinstance(event, RoundEvent):
        print(event.round, event.is_final, len(event.tool_calls))
```

### Memory and cache

Persistent memory:

```python
from sql_to_graph import AgentMemory, DataAnalystAgent, QueryCache, create_llm

memory = AgentMemory(path="/tmp/sql_to_graph_memory.json", max_entries=200)
cache = QueryCache(max_size=100)

agent = DataAnalystAgent(
    connection_string="postgresql://...",
    llm=create_llm("openai", model="gpt-4o"),
    memory=memory,
    cache=cache,
)
```

Memory stores:

- prior queries and intent,
- learned facts,
- user preferences.

The agent can search memory through the built-in `sql_recall_queries` tool.

### Planning and reflection

For more complex questions you can enable:

- `use_planner=True` to split a question into multiple sub-queries,
- `use_reflection=True` to review the generated answer and retry if needed.

Relevant exported pieces:

- `QueryPlanner`
- `ParallelExecutor`
- `Synthesizer`
- `needs_planning(...)`
- `ReflectionAgent`
- `create_langgraph_agent(...)`

### Prompt customization

Use `custom_prompt` to inject domain-specific constraints:

```python
agent = DataAnalystAgent(
    connection_string="postgresql://...",
    llm=create_llm("anthropic", model="claude-sonnet-4-20250514"),
    custom_prompt=(
        "Revenue is stored in cents.\n"
        "Always filter to tenant_id = 42 unless the user asks otherwise."
    ),
)
```

## LLM and tool integration

### LLM factory

The exported factory is:

- `create_llm("anthropic", ...)`
- `create_llm("openai", ...)`
- `create_llm("langchain", llm=...)`

The returned object implements the `UnifiedLLM` protocol.

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
