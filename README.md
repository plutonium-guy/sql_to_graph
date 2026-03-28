# sql-to-graph

Rust-powered SQL auto-correction, optimization, execution, and chart generation. Built for AI agents.

```
pip install sql-to-graph
```

## What it does

1. **Auto-corrects** SQL queries using an LLM + actual database schema
2. **Optimizes** queries via AST rewrites (constant folding, boolean simplification, LIMIT pushdown)
3. **Executes** on PostgreSQL, MySQL, or SQLite
4. **Generates charts** from query results (HTML, PNG, JPG, SVG)

Heavy lifting runs in Rust. Python gets a clean async API.

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

## Chart types

| Type | Enum |
|------|------|
| Bar | `ChartType.Bar` |
| Horizontal Bar | `ChartType.HorizontalBar` |
| Stacked Bar | `ChartType.StackedBar` |
| Line | `ChartType.Line` |
| Area | `ChartType.Area` |
| Pie | `ChartType.Pie` |
| Donut | `ChartType.Donut` |
| Scatter | `ChartType.Scatter` |
| Histogram | `ChartType.Histogram` |
| Heatmap | `ChartType.Heatmap` |

Output formats: `OutputFormat.Html`, `OutputFormat.Png`, `OutputFormat.Jpg`, `OutputFormat.Svg`

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
from sql_to_graph import handle_tool_call, handle_discovery_call

# For sql_to_graph tool calls
response = await handle_tool_call(
    arguments=tool_call.arguments,
    llm=llm,    # optional: enables auto-correction
    cache=cache, # optional: caches results
)

# For discovery tool calls (schemas, describe, sample)
response = await handle_discovery_call(
    tool_name="sql_describe_table",
    arguments={"connection_string": "...", "table": "users"},
)
```

## Schema discovery

```python
from sql_to_graph import Connection

conn = Connection("postgresql://...", read_only=True)
await conn.connect()

schemas = await conn.list_schemas()
metadata = await conn.get_metadata("public")     # all tables in schema
table = await conn.describe_table("users")        # columns, types, row estimate
sample = await conn.sample_table("users", n=5)    # sample rows
```

## Statistics and chart suggestions

```python
from sql_to_graph import summarize_result, suggest_charts

result = await conn.execute("SELECT * FROM sales")

# Column statistics: min, max, mean, median, stddev, nulls, distinct count
summary = summarize_result(result)
for stat in summary.column_stats:
    print(f"{stat.column_name}: mean={stat.mean}, nulls={stat.null_count}")

# Auto-suggest best chart types for the data
suggestions = suggest_charts(result)
for s in suggestions:
    print(f"{s.chart_type} - {s.title} (confidence: {s.confidence})")
```

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

## Query cache

```python
from sql_to_graph import QueryCache

cache = QueryCache(max_size=100)

# Use with agent handler
response = await handle_tool_call(arguments=args, cache=cache)
# Second call with same SQL returns cached result
```

## Databases

| Database | Connection string |
|----------|-------------------|
| PostgreSQL | `postgresql://user:pass@host:5432/dbname` |
| MySQL | `mysql://user:pass@host:3306/dbname` |
| SQLite | `sqlite:///path/to/db.sqlite` |

## License

MIT
