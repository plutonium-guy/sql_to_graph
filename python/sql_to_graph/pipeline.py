"""High-level pipeline: SQL in, results + chart out."""

from __future__ import annotations

import asyncio

from sql_to_graph._native import (
    ChartConfig,
    ChartOutput,
    Connection,
    QueryResult,
    apply_correction,
    build_correction_context,
    optimize_query,
    parse_sql,
    render_chart,
)
from sql_to_graph.llm import LLMProvider


async def sql_to_chart(
    sql: str,
    connection_string: str,
    llm: LLMProvider | None = None,
    chart_config: ChartConfig | None = None,
    auto_correct: bool = True,
    optimize: bool = True,
    schema: str | None = None,
    read_only: bool = True,
) -> tuple[QueryResult, ChartOutput | None]:
    """One-call convenience function: SQL in, results + optional chart out."""
    conn = Connection(connection_string, read_only=read_only, schema=schema)
    await conn.connect()

    try:
        dialect = conn.dialect
        current_sql = sql

        # Step 1: Parse and check for errors
        parse_result = parse_sql(current_sql, dialect)

        # Step 2: Auto-correct if needed
        if auto_correct and llm is not None and not parse_result.is_valid:
            metadata = await conn.get_metadata(schema)
            context = build_correction_context(current_sql, metadata, dialect)
            corrected = await llm.complete(context.suggested_prompt)
            current_sql = apply_correction(current_sql, corrected, dialect)
        elif auto_correct and llm is not None:
            metadata = await conn.get_metadata(schema)
            context = build_correction_context(current_sql, metadata, dialect)
            if context.parse_errors:
                corrected = await llm.complete(context.suggested_prompt)
                current_sql = apply_correction(current_sql, corrected, dialect)

        # Step 3: Optimize
        if optimize:
            current_sql = optimize_query(current_sql, dialect)

        # Step 4: Execute
        result = await conn.execute(current_sql)

        # Step 5: Generate chart
        chart_output = None
        if chart_config is not None:
            chart_output = render_chart(result, chart_config)

        return result, chart_output

    finally:
        await conn.close()


def sql_to_chart_sync(
    sql: str,
    connection_string: str,
    llm: LLMProvider | None = None,
    chart_config: ChartConfig | None = None,
    auto_correct: bool = True,
    optimize: bool = True,
    schema: str | None = None,
    read_only: bool = True,
) -> tuple[QueryResult, ChartOutput | None]:
    """Synchronous wrapper around sql_to_chart."""
    return asyncio.run(
        sql_to_chart(
            sql=sql,
            connection_string=connection_string,
            llm=llm,
            chart_config=chart_config,
            auto_correct=auto_correct,
            optimize=optimize,
            schema=schema,
            read_only=read_only,
        )
    )
