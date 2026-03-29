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
from sql_to_graph.memory import AgentMemory, MemoryEntry
from sql_to_graph.langchain_tools import get_langchain_tools
from sql_to_graph.react_agent import (
    AgentResponse,
    DataAnalystAgent,
    PlanEvent,
    ReflectionEvent,
    RoundEvent,
    ToolCallEvent,
    build_schema_ddl,
    create_langgraph_agent,
)
from sql_to_graph.llm_factory import (
    AnthropicLLM,
    ChatWithToolsResult,
    LangChainLLM,
    OpenAILLM,
    ToolCallRequest,
    ToolResultMessage,
    UnifiedLLM,
    create_llm,
    from_legacy,
)
from sql_to_graph.planner import (
    ParallelExecutor,
    QueryPlan,
    QueryPlanner,
    QueryStep,
    StepResult,
    Synthesizer,
    needs_planning,
)
from sql_to_graph.reflector import ReflectionAgent, ReflectionResult
from sql_to_graph.toons import (
    ToonsConfig,
    compare_token_usage,
    toons_decode,
    toons_encode,
)

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
    # Memory
    "AgentMemory",
    "MemoryEntry",
    # LangChain
    "get_langchain_tools",
    # React agent
    "DataAnalystAgent",
    "AgentResponse",
    "ToolCallEvent",
    "RoundEvent",
    "ReflectionEvent",
    "PlanEvent",
    "build_schema_ddl",
    "create_langgraph_agent",
    # LLM factory
    "UnifiedLLM",
    "create_llm",
    "from_legacy",
    "AnthropicLLM",
    "OpenAILLM",
    "LangChainLLM",
    "ChatWithToolsResult",
    "ToolCallRequest",
    "ToolResultMessage",
    # Planner
    "QueryPlanner",
    "ParallelExecutor",
    "Synthesizer",
    "QueryPlan",
    "QueryStep",
    "StepResult",
    "needs_planning",
    # Reflector
    "ReflectionAgent",
    "ReflectionResult",
    # TOONS
    "ToonsConfig",
    "toons_encode",
    "toons_decode",
    "compare_token_usage",
]
