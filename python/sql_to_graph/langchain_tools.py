"""LangChain tool wrappers for sql_to_graph.

Usage:
    from sql_to_graph import get_langchain_tools

    tools = get_langchain_tools(
        connection_string="postgresql://user:pass@localhost/db",
    )
    # Pass `tools` to any LangChain agent
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Optional, Type

try:
    from langchain_core.tools import BaseTool
    from langchain_core.callbacks import CallbackManagerForToolRun, AsyncCallbackManagerForToolRun
    from pydantic import BaseModel, Field

    HAS_LANGCHAIN = True
except ImportError:
    HAS_LANGCHAIN = False


def _check_langchain():
    if not HAS_LANGCHAIN:
        raise ImportError(
            "langchain-core and pydantic are required for LangChain tools. "
            "Install with: pip install 'sql-to-graph[langchain]'"
        )


# ─── Pydantic input schemas ───────────────────────────────────────────────

if HAS_LANGCHAIN:

    class SqlQueryInput(BaseModel):
        sql: str = Field(description="The SQL query to execute.")
        schema_name: Optional[str] = Field(
            default=None,
            description="Database schema to use (e.g., 'public'). Defaults to 'public' for PostgreSQL.",
        )
        limit: Optional[int] = Field(
            default=None,
            description="Maximum number of rows to return.",
        )
        offset: int = Field(
            default=0,
            description="Number of rows to skip (for pagination).",
        )
        include_stats: bool = Field(
            default=False,
            description="Include column statistics in the response.",
        )
        suggest_charts: bool = Field(
            default=False,
            description="Include chart type suggestions based on the data.",
        )
        export_format: Optional[str] = Field(
            default=None,
            description="Export format: 'csv' or 'json'. Returns raw data instead of rows.",
        )

    class DiscoverSchemasInput(BaseModel):
        pass

    class DescribeTableInput(BaseModel):
        table: str = Field(description="Table name to describe.")
        schema_name: Optional[str] = Field(
            default=None,
            description="Schema name (optional, defaults to 'public' for PostgreSQL).",
        )

    class SampleDataInput(BaseModel):
        table: str = Field(description="Table name to sample.")
        n: int = Field(default=5, description="Number of sample rows to return.")
        schema_name: Optional[str] = Field(
            default=None,
            description="Schema name (optional).",
        )

    # ─── Tool classes ──────────────────────────────────────────────────────

    class SqlQueryTool(BaseTool):
        name: str = "sql_query"
        description: str = (
            "Execute a SQL query against the database with auto-correction and optimization. "
            "Returns query results with columns and rows. Can include statistics and chart suggestions."
        )
        args_schema: Type[BaseModel] = SqlQueryInput

        connection_string: str
        llm: Any = None
        cache: Any = None

        async def _arun(
            self,
            sql: str,
            schema_name: str | None = None,
            limit: int | None = None,
            offset: int = 0,
            include_stats: bool = False,
            suggest_charts: bool = False,
            export_format: str | None = None,
            run_manager: AsyncCallbackManagerForToolRun | None = None,
        ) -> str:
            from sql_to_graph.agent import handle_tool_call

            arguments = {
                "sql": sql,
                "connection_string": self.connection_string,
                "schema": schema_name,
                "auto_correct": self.llm is not None,
                "optimize": True,
                "limit": limit,
                "offset": offset,
                "include_stats": include_stats,
                "suggest_charts": suggest_charts,
                "export_format": export_format,
            }
            result = await handle_tool_call(arguments, llm=self.llm, cache=self.cache)
            return json.dumps(result, default=str)

        def _run(
            self,
            sql: str,
            schema_name: str | None = None,
            limit: int | None = None,
            offset: int = 0,
            include_stats: bool = False,
            suggest_charts: bool = False,
            export_format: str | None = None,
            run_manager: CallbackManagerForToolRun | None = None,
        ) -> str:
            return asyncio.run(
                self._arun(
                    sql=sql,
                    schema_name=schema_name,
                    limit=limit,
                    offset=offset,
                    include_stats=include_stats,
                    suggest_charts=suggest_charts,
                    export_format=export_format,
                )
            )

    class DiscoverSchemasTool(BaseTool):
        name: str = "sql_discover_schemas"
        description: str = (
            "List all available schemas/databases. "
            "Use this first to understand what schemas are available before writing queries."
        )
        args_schema: Type[BaseModel] = DiscoverSchemasInput

        connection_string: str

        async def _arun(
            self,
            run_manager: AsyncCallbackManagerForToolRun | None = None,
        ) -> str:
            from sql_to_graph.agent import handle_discovery_call

            result = await handle_discovery_call(
                "sql_discover_schemas",
                {"connection_string": self.connection_string},
            )
            return json.dumps(result, default=str)

        def _run(
            self,
            run_manager: CallbackManagerForToolRun | None = None,
        ) -> str:
            return asyncio.run(self._arun())

    class DescribeTableTool(BaseTool):
        name: str = "sql_describe_table"
        description: str = (
            "Get detailed metadata for a table: column names, data types, nullability, "
            "and estimated row count."
        )
        args_schema: Type[BaseModel] = DescribeTableInput

        connection_string: str

        async def _arun(
            self,
            table: str,
            schema_name: str | None = None,
            run_manager: AsyncCallbackManagerForToolRun | None = None,
        ) -> str:
            from sql_to_graph.agent import handle_discovery_call

            result = await handle_discovery_call(
                "sql_describe_table",
                {
                    "connection_string": self.connection_string,
                    "table": table,
                    "schema": schema_name,
                },
            )
            return json.dumps(result, default=str)

        def _run(
            self,
            table: str,
            schema_name: str | None = None,
            run_manager: CallbackManagerForToolRun | None = None,
        ) -> str:
            return asyncio.run(self._arun(table=table, schema_name=schema_name))

    class SampleDataTool(BaseTool):
        name: str = "sql_sample_data"
        description: str = (
            "Get sample rows from a table. Use this to understand the data "
            "before writing queries."
        )
        args_schema: Type[BaseModel] = SampleDataInput

        connection_string: str

        async def _arun(
            self,
            table: str,
            n: int = 5,
            schema_name: str | None = None,
            run_manager: AsyncCallbackManagerForToolRun | None = None,
        ) -> str:
            from sql_to_graph.agent import handle_discovery_call

            result = await handle_discovery_call(
                "sql_sample_data",
                {
                    "connection_string": self.connection_string,
                    "table": table,
                    "n": n,
                    "schema": schema_name,
                },
            )
            return json.dumps(result, default=str)

        def _run(
            self,
            table: str,
            n: int = 5,
            schema_name: str | None = None,
            run_manager: CallbackManagerForToolRun | None = None,
        ) -> str:
            return asyncio.run(self._arun(table=table, n=n, schema_name=schema_name))


def get_langchain_tools(
    connection_string: str,
    llm: Any = None,
    cache: Any = None,
) -> list:
    """Create LangChain tools for sql_to_graph.

    Args:
        connection_string: Database connection URL.
        llm: Optional LLMProvider for SQL auto-correction.
        cache: Optional QueryCache for caching results.

    Returns:
        List of LangChain BaseTool instances ready to use with any LangChain agent.

    Example:
        from sql_to_graph import get_langchain_tools
        from langchain_openai import ChatOpenAI
        from langgraph.prebuilt import create_react_agent

        tools = get_langchain_tools("postgresql://user:pass@localhost/db")
        agent = create_react_agent(ChatOpenAI(model="gpt-4o"), tools)
    """
    _check_langchain()
    return [
        SqlQueryTool(connection_string=connection_string, llm=llm, cache=cache),
        DiscoverSchemasTool(connection_string=connection_string),
        DescribeTableTool(connection_string=connection_string),
        SampleDataTool(connection_string=connection_string),
    ]
