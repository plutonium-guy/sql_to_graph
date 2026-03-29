"""Reflection agent for reviewing data analysis answers.

The reflector examines the agent's output and decides whether it
adequately answers the user's question, or needs a retry with feedback.

Usage::

    from sql_to_graph.reflector import ReflectionAgent, ReflectionResult

    reflector = ReflectionAgent(llm=create_llm("anthropic", model="claude-haiku-4-20250414"))
    result = await reflector.reflect(
        question="What are the top 5 customers?",
        answer=agent_response.text,
        sql_executed=agent_response.sql_executed,
        errors=agent_response.errors,
        schema_ddl=agent.schema_ddl,
    )
    if not result.accepted:
        # Retry with feedback
        response = await agent.chat(f"{question}\\n\\nFeedback: {result.critique}")
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sql_to_graph.toons import ToonsConfig, toons_encode

if TYPE_CHECKING:
    from sql_to_graph.llm_factory import UnifiedLLM

logger = logging.getLogger("sql_to_graph.reflector")


@dataclass
class ReflectionResult:
    """Output from the reflection agent."""

    accepted: bool
    critique: str | None = None
    retry_hint: str | None = None
    improved_text: str | None = None


_REFLECTION_PROMPT = """\
You are a data analysis reviewer. Evaluate whether the agent's answer
correctly and completely addresses the user's question.

## User's Question
{question}

## SQL Executed
{sql}

## Agent's Answer
{answer}

## Errors Encountered
{errors}

## Database Schema (excerpt)
{schema_excerpt}

## Evaluation Criteria

1. **Correctness**: Does the SQL logically answer the question asked?
   - Are GROUP BY / WHERE / JOIN / aggregations correct?
   - Does the answer reference the right tables and columns?
2. **Completeness**: Does the answer fully address all parts of the question?
   - If the user asked "compare A and B", are both addressed?
   - If the user asked for a chart, was one generated?
3. **Accuracy**: Are the numbers in the answer consistent with the SQL?
   - Does the answer misinterpret or misquote results?
4. **Data Quality**: Are there obvious issues?
   - Did the agent ignore high null rates or suspicious data?
   - Were warnings from statistics acknowledged?
5. **Relevance**: Is the visualization appropriate for the data?

## Response Format (JSON only)

```json
{{
  "accepted": true or false,
  "critique": "What's wrong (null if accepted)",
  "retry_hint": "Specific instruction for fix (null if accepted)",
  "improved_text": "Improved answer text (null if no improvement needed)"
}}
```

Be strict but fair. Reject only for genuine issues, not style preferences.
Respond with ONLY the JSON, no other text."""


class ReflectionAgent:
    """Reviews agent answers and optionally triggers retries.

    Args:
        llm: UnifiedLLM instance. A cheap/fast model works well here
            since reflection doesn't require tool calling.
        max_retries: Maximum number of retry attempts if reflection fails.
        skip_simple: If True, skip reflection for simple single-query answers.
    """

    def __init__(
        self,
        llm: UnifiedLLM,
        max_retries: int = 1,
        skip_simple: bool = True,
    ):
        self._llm = llm
        self.max_retries = max_retries
        self.skip_simple = skip_simple

    async def reflect(
        self,
        question: str,
        answer: str,
        sql_executed: str | None = None,
        errors: list[dict] | None = None,
        schema_ddl: str | None = None,
    ) -> ReflectionResult:
        """Review an agent's answer.

        Args:
            question: The user's original question.
            answer: The agent's generated answer text.
            sql_executed: The SQL query that was run.
            errors: List of error dicts from the agent run.
            schema_ddl: Database schema DDL for context.

        Returns:
            :class:`ReflectionResult` with accept/reject verdict.
        """
        # Truncate schema to keep prompt reasonable
        schema_excerpt = (schema_ddl or "Not available")[:2000]
        if schema_ddl and len(schema_ddl) > 2000:
            schema_excerpt += "\n... (truncated)"

        errors_text = "None"
        if errors:
            # Use TOONS for compact error representation
            err_parts: list[str] = []
            for err in errors[:5]:  # cap at 5 errors
                err_parts.append(toons_encode(err, ToonsConfig(max_rows=10)))
            errors_text = "\n".join(err_parts)

        prompt = _REFLECTION_PROMPT.format(
            question=question,
            sql=sql_executed or "None",
            answer=answer[:3000],
            errors=errors_text,
            schema_excerpt=schema_excerpt,
        )

        raw = await self._llm.complete(
            prompt, system="You are a data analysis reviewer. Output valid JSON only."
        )

        result = self._parse_result(raw)

        logger.info(
            "Reflection: accepted=%s, critique=%s",
            result.accepted,
            (result.critique or "")[:100],
        )
        return result

    def should_reflect(
        self,
        rounds_used: int,
        errors: list[dict] | None = None,
        sql_executed: str | None = None,
    ) -> bool:
        """Heuristic: should we run reflection on this response?

        Skips reflection for simple, error-free single-round queries
        when ``skip_simple`` is enabled.
        """
        # Always reflect if there were errors
        if errors:
            return True

        # Skip simple queries
        if self.skip_simple and rounds_used <= 2:
            sql = (sql_executed or "").upper()
            # Simple query heuristic: single SELECT, no subqueries/JOINs
            if sql.count("SELECT") == 1 and "JOIN" not in sql and "WITH" not in sql:
                return False

        return True

    def _parse_result(self, raw: str) -> ReflectionResult:
        """Parse the LLM's JSON response."""
        raw = raw.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)

        try:
            data = json.loads(raw)
            return ReflectionResult(
                accepted=bool(data.get("accepted", True)),
                critique=data.get("critique"),
                retry_hint=data.get("retry_hint"),
                improved_text=data.get("improved_text"),
            )
        except (json.JSONDecodeError, TypeError):
            logger.warning("Failed to parse reflection JSON, assuming accepted")
            return ReflectionResult(accepted=True)
