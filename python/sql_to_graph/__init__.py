"""sql_to_graph - SQL auto-correction, optimization, execution, and chart generation."""

from sql_to_graph._native import (
    # Types
    SqlDialect,
    ChartType,
    OutputFormat,
    ColumnCategory,
    SchemaInfo,
    ColumnInfo,
    TableMetadata,
    CellValue,
    QueryResult,
    ChartConfig,
    ChartOutput,
    CorrectionContext,
    ParseResult,
    ColumnStats,
    ResultSummary,
    ChartSuggestion,
    EnrichedError,
    # Connection
    Connection,
    # SQL functions
    parse_sql,
    build_correction_context,
    apply_correction,
    optimize_query,
    # Chart functions
    render_chart,
    # Statistics & suggestions
    summarize_result,
    suggest_charts,
    # Export
    export_csv,
    export_json,
    # Agent functions
    get_tool_schema,
    get_tool_definition,
)
from sql_to_graph.pipeline import sql_to_chart, sql_to_chart_sync
from sql_to_graph.llm import LLMProvider, LangChainProvider
from sql_to_graph.agent import (
    as_openai_tool,
    as_anthropic_tool,
    as_openai_tools,
    as_anthropic_tools,
    as_mcp_tools,
    handle_tool_call,
    handle_discovery_call,
)
from sql_to_graph.cache import QueryCache

__all__ = [
    # Types
    "SqlDialect",
    "ChartType",
    "OutputFormat",
    "ColumnCategory",
    "SchemaInfo",
    "ColumnInfo",
    "TableMetadata",
    "CellValue",
    "QueryResult",
    "ChartConfig",
    "ChartOutput",
    "CorrectionContext",
    "ParseResult",
    "ColumnStats",
    "ResultSummary",
    "ChartSuggestion",
    "EnrichedError",
    # Connection
    "Connection",
    # SQL functions
    "parse_sql",
    "build_correction_context",
    "apply_correction",
    "optimize_query",
    # Chart functions
    "render_chart",
    # Statistics & suggestions
    "summarize_result",
    "suggest_charts",
    # Export
    "export_csv",
    "export_json",
    # Agent functions
    "get_tool_schema",
    "get_tool_definition",
    "as_openai_tool",
    "as_anthropic_tool",
    "as_openai_tools",
    "as_anthropic_tools",
    "as_mcp_tools",
    "handle_tool_call",
    "handle_discovery_call",
    # Pipeline
    "sql_to_chart",
    "sql_to_chart_sync",
    # LLM
    "LLMProvider",
    "LangChainProvider",
    # Cache
    "QueryCache",
]
