"""Agent framework adapters for sql_to_graph."""

from __future__ import annotations

import json
from typing import Any

from sql_to_graph._native import (
    ChartConfig,
    ChartType,
    Connection,
    OutputFormat,
    apply_correction,
    build_correction_context,
    export_csv,
    export_json,
    get_tool_definition,
    get_tool_schema,
    optimize_query,
    render_chart,
    suggest_charts,
    summarize_result,
)
from sql_to_graph.cache import QueryCache
from sql_to_graph.llm import LLMProvider


# ─── Single tool (backwards compatible) ─────────────────────────────────────

def as_openai_tool() -> dict:
    """Return a single OpenAI function-calling compatible tool definition."""
    schema = json.loads(get_tool_schema())
    return {
        "type": "function",
        "function": {
            "name": "sql_to_graph",
            "description": (
                "Execute a SQL query against a database with auto-correction, "
                "optimization, and optional chart generation."
            ),
            "parameters": schema,
        },
    }


def as_anthropic_tool() -> dict:
    """Return a single Anthropic tool-use compatible tool definition."""
    schema = json.loads(get_tool_schema())
    return {
        "name": "sql_to_graph",
        "description": (
            "Execute a SQL query against a database with auto-correction, "
            "optimization, and optional chart generation."
        ),
        "input_schema": schema,
    }


# ─── Multi-tool (full agent toolkit) ────────────────────────────────────────

def as_openai_tools() -> list[dict]:
    """Return ALL tools as OpenAI function-calling compatible definitions."""
    tools_json = json.loads(get_tool_definition())
    return [
        {
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t["description"],
                "parameters": t["input_schema"],
            },
        }
        for t in tools_json
    ]


def as_anthropic_tools() -> list[dict]:
    """Return ALL tools as Anthropic tool-use compatible definitions."""
    return json.loads(get_tool_definition())


def as_mcp_tools() -> list[dict]:
    """Return ALL tools as MCP-compatible definitions."""
    return json.loads(get_tool_definition())


# ─── Chart type / format maps ───────────────────────────────────────────────

_CHART_TYPE_MAP = {
    "bar": ChartType.Bar,
    "horizontal_bar": ChartType.HorizontalBar,
    "stacked_bar": ChartType.StackedBar,
    "line": ChartType.Line,
    "area": ChartType.Area,
    "pie": ChartType.Pie,
    "donut": ChartType.Donut,
    "scatter": ChartType.Scatter,
    "histogram": ChartType.Histogram,
    "heatmap": ChartType.Heatmap,
}

_FORMAT_MAP = {
    "html": OutputFormat.Html,
    "png": OutputFormat.Png,
    "jpg": OutputFormat.Jpg,
    "svg": OutputFormat.Svg,
}


# ─── Universal tool call handler ─────────────────────────────────────────────

async def handle_tool_call(
    arguments: dict[str, Any],
    llm: LLMProvider | None = None,
    cache: QueryCache | None = None,
) -> dict[str, Any]:
    """Universal handler for sql_to_graph agent tool calls."""
    sql = arguments["sql"]
    connection_string = arguments["connection_string"]
    schema = arguments.get("schema")
    auto_correct = arguments.get("auto_correct", True)
    should_optimize = arguments.get("optimize", True)
    limit = arguments.get("limit")
    offset = arguments.get("offset", 0)
    include_stats = arguments.get("include_stats", False)
    should_suggest = arguments.get("suggest_charts", False)
    export_format = arguments.get("export_format")
    chart_args = arguments.get("chart")

    # Check cache
    if cache is not None:
        cached = cache.get(sql)
        if cached is not None:
            return _build_response(
                cached, sql, include_stats, should_suggest, export_format, chart_args, from_cache=True
            )

    conn = Connection(connection_string, read_only=True, schema=schema)
    await conn.connect()

    try:
        # Auto-correct
        if auto_correct and llm is not None:
            metadata = await conn.get_metadata(schema)
            context = build_correction_context(sql, metadata)
            corrected = await llm.complete(context.suggested_prompt)
            sql = apply_correction(sql, corrected)

        # Optimize
        if should_optimize:
            sql = optimize_query(sql, conn.dialect)

        # Execute (paginated or full)
        if limit is not None:
            result = await conn.execute_paginated(sql, limit, offset)
        else:
            result = await conn.execute(sql)

        # Cache
        if cache is not None:
            cache.put(sql, result)

        return _build_response(
            result, sql, include_stats, should_suggest, export_format, chart_args
        )

    finally:
        await conn.close()


def _build_response(
    result,
    sql: str,
    include_stats: bool,
    should_suggest: bool,
    export_format: str | None,
    chart_args: dict | None,
    from_cache: bool = False,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "sql_executed": sql,
        "columns": result.columns,
        "row_count": result.row_count,
        "execution_time_ms": result.execution_time_ms,
        "from_cache": from_cache,
    }

    if result.total_row_count is not None:
        response["total_row_count"] = result.total_row_count
        response["has_more"] = result.has_more

    # Export
    if export_format == "csv":
        response["export_data"] = export_csv(result).decode("utf-8")
        response["export_format"] = "csv"
    elif export_format == "json":
        response["export_data"] = export_json(result)
        response["export_format"] = "json"
    else:
        # Include rows (capped at 100 for agent responses)
        response["rows"] = [
            {col: cell.to_python() for col, cell in zip(result.columns, row)}
            for row in result.rows[:100]
        ]

    # Stats
    if include_stats:
        summary = summarize_result(result)
        response["statistics"] = {
            "columns": [
                {
                    "name": s.column_name,
                    "category": str(s.category),
                    "null_count": s.null_count,
                    "distinct_count": s.distinct_count,
                    "min": s.min,
                    "max": s.max,
                    "mean": s.mean,
                    "median": s.median,
                    "stddev": s.stddev,
                    "top_values": s.top_values if s.top_values else None,
                }
                for s in summary.column_stats
            ],
            "warnings": summary.warnings,
        }

    # Chart suggestions
    if should_suggest:
        suggestions = suggest_charts(result)
        response["chart_suggestions"] = [
            {
                "chart_type": str(s.chart_type),
                "x_column": s.x_column,
                "y_column": s.y_column,
                "z_column": s.z_column,
                "title": s.title,
                "confidence": s.confidence,
                "reasoning": s.reasoning,
            }
            for s in suggestions
        ]

    # Chart rendering
    if chart_args:
        chart_config = ChartConfig(
            chart_type=_CHART_TYPE_MAP[chart_args["type"]],
            x_column=chart_args["x_column"],
            y_column=chart_args["y_column"],
            title=chart_args.get("title"),
            z_column=chart_args.get("z_column"),
            bin_count=chart_args.get("bin_count", 10),
            output_format=_FORMAT_MAP.get(chart_args.get("format", "html"), OutputFormat.Html),
        )
        chart_output = render_chart(result, chart_config)
        response["chart"] = {
            "format": chart_args.get("format", "html"),
            "mime_type": chart_output.mime_type,
            "size_bytes": len(chart_output.data),
        }

    return response


# ─── Discovery tool handlers ────────────────────────────────────────────────

async def handle_discovery_call(
    tool_name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle schema discovery tool calls."""
    connection_string = arguments["connection_string"]
    conn = Connection(connection_string, read_only=True)
    await conn.connect()

    try:
        if tool_name == "sql_discover_schemas":
            schemas = await conn.list_schemas()
            return {
                "schemas": [
                    {"name": s.name, "table_count": s.table_count} for s in schemas
                ]
            }

        elif tool_name == "sql_describe_table":
            table = arguments["table"]
            schema = arguments.get("schema")
            meta = await conn.describe_table(table, schema)
            return {
                "schema": meta.schema_name,
                "table": meta.table_name,
                "row_count_estimate": meta.row_count_estimate,
                "columns": [
                    {
                        "name": c.name,
                        "data_type": c.data_type,
                        "is_nullable": c.is_nullable,
                    }
                    for c in meta.columns
                ],
            }

        elif tool_name == "sql_sample_data":
            table = arguments["table"]
            n = arguments.get("n", 5)
            schema = arguments.get("schema")
            result = await conn.sample_table(table, n, schema)
            return {
                "columns": result.columns,
                "row_count": result.row_count,
                "rows": [
                    {col: cell.to_python() for col, cell in zip(result.columns, row)}
                    for row in result.rows
                ],
            }

        else:
            return {"error": f"Unknown tool: {tool_name}"}

    finally:
        await conn.close()
