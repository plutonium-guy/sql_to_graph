"""Query planner, parallel executor, and synthesizer for multi-step analysis.

Instead of the React agent running one query at a time with an LLM round-trip
per query, the planner decomposes a complex question upfront, the executor
runs independent queries in parallel, and the synthesizer merges all results
into a single coherent answer.

Typical flow::

    Planner  (1 LLM call)  →  Executor  (0 LLM calls, N SQL queries)
        →  Synthesizer  (1 LLM call)  →  AgentResponse
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sql_to_graph.toons import ToonsConfig, toons_encode

if TYPE_CHECKING:
    from sql_to_graph.llm_factory import UnifiedLLM
    from sql_to_graph.react_agent import AgentResponse

logger = logging.getLogger("sql_to_graph.planner")


# ─── Data types ──────────────────────────────────────────────────────────

@dataclass
class QueryStep:
    """One step in a multi-query plan."""

    id: str
    sub_question: str
    depends_on: list[str] = field(default_factory=list)
    context_hint: str | None = None
    parallel_group: int = 0


@dataclass
class QueryPlan:
    """A planned decomposition of a complex question."""

    original_question: str
    steps: list[QueryStep]
    reasoning: str = ""

    @property
    def is_simple(self) -> bool:
        """True if the plan has only one step (skip planner overhead)."""
        return len(self.steps) <= 1


@dataclass
class StepResult:
    """Result of executing one step."""

    step: QueryStep
    response: Any  # AgentResponse
    error: str | None = None


# ─── Planner ─────────────────────────────────────────────────────────────

_PLAN_PROMPT = """\
You are a SQL query planner. Given a user question and database schema,
decompose the question into the minimum number of independent sub-questions
that can be answered with SQL queries.

## Rules

1. If a single query can answer the question, return exactly ONE step.
   Do NOT over-decompose simple questions.
2. Mark steps as depending on earlier steps ONLY when the later step
   genuinely needs data from the earlier step's result.
3. Independent steps (no dependency) will run in parallel for speed.
4. Each sub_question should be self-contained and answerable by a data
   analyst agent with access to the database.
5. Include a context_hint for dependent steps explaining what data they
   need from the prior step.

## Database Schema

{schema_ddl}

## User Question

{question}

## Response Format (JSON only)

```json
{{
  "reasoning": "Brief explanation of why you decomposed this way",
  "steps": [
    {{
      "id": "step_1",
      "sub_question": "Natural language question for the agent",
      "depends_on": [],
      "context_hint": null
    }},
    {{
      "id": "step_2",
      "sub_question": "Follow-up question using step_1 results",
      "depends_on": ["step_1"],
      "context_hint": "Use the top region from step_1"
    }}
  ]
}}
```

Respond with ONLY the JSON, no other text."""


class QueryPlanner:
    """Decomposes complex questions into a query plan.

    Args:
        llm: UnifiedLLM instance (can be a cheap/fast model like claude-haiku).
    """

    def __init__(self, llm: UnifiedLLM):
        self._llm = llm

    async def plan(self, question: str, schema_ddl: str) -> QueryPlan:
        """Create a query plan for the given question.

        Args:
            question: The user's natural language question.
            schema_ddl: Database schema DDL text.

        Returns:
            A :class:`QueryPlan` with one or more steps.
        """
        prompt = _PLAN_PROMPT.format(
            schema_ddl=schema_ddl,
            question=question,
        )

        raw = await self._llm.complete(
            prompt, system="You are a SQL query planner. Output valid JSON only."
        )

        plan = self._parse_plan(raw, question)
        plan = self._assign_parallel_groups(plan)

        logger.info(
            "Planned %d steps for question: %s (simple=%s)",
            len(plan.steps), question[:80], plan.is_simple,
        )
        return plan

    def _parse_plan(self, raw: str, question: str) -> QueryPlan:
        """Parse the LLM's JSON response into a QueryPlan."""
        # Strip markdown code fences if present
        raw = raw.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Failed to parse plan JSON, falling back to single step")
            return QueryPlan(
                original_question=question,
                steps=[QueryStep(id="step_1", sub_question=question)],
                reasoning="Plan parsing failed; using original question as single step.",
            )

        steps: list[QueryStep] = []
        for s in data.get("steps", []):
            steps.append(QueryStep(
                id=s.get("id", f"step_{len(steps)+1}"),
                sub_question=s.get("sub_question", question),
                depends_on=s.get("depends_on", []),
                context_hint=s.get("context_hint"),
            ))

        if not steps:
            steps = [QueryStep(id="step_1", sub_question=question)]

        return QueryPlan(
            original_question=question,
            steps=steps,
            reasoning=data.get("reasoning", ""),
        )

    def _assign_parallel_groups(self, plan: QueryPlan) -> QueryPlan:
        """Topological sort into parallel execution groups."""
        completed: set[str] = set()
        remaining = list(plan.steps)
        group = 0

        while remaining:
            ready = [
                s for s in remaining
                if all(d in completed for d in s.depends_on)
            ]
            if not ready:
                # Circular or unresolvable deps — run rest sequentially
                for s in remaining:
                    s.parallel_group = group
                    group += 1
                break

            for s in ready:
                s.parallel_group = group
                completed.add(s.id)
            remaining = [s for s in remaining if s.id not in completed]
            group += 1

        return plan


# ─── Parallel Executor ───────────────────────────────────────────────────

class ParallelExecutor:
    """Executes a query plan, running independent steps concurrently.

    Args:
        agent_chat_fn: An async callable ``(question: str) -> AgentResponse``
            that runs the React agent for a single sub-question. Typically
            ``agent.chat_isolated``.
    """

    def __init__(self, agent_chat_fn: Any):
        self._chat = agent_chat_fn

    async def execute(self, plan: QueryPlan) -> list[StepResult]:
        """Execute all steps in the plan with maximum parallelism.

        Steps in the same ``parallel_group`` run concurrently. Groups
        execute in order.

        Returns:
            Ordered list of :class:`StepResult`.
        """
        # Group steps by parallel_group
        groups: dict[int, list[QueryStep]] = {}
        for step in plan.steps:
            groups.setdefault(step.parallel_group, []).append(step)

        all_results: dict[str, StepResult] = {}

        for group_id in sorted(groups.keys()):
            group_steps = groups[group_id]

            # Inject context from prior steps into sub-questions
            enriched: list[QueryStep] = []
            for step in group_steps:
                question = step.sub_question
                if step.depends_on and step.context_hint:
                    prior_context = self._build_context(step.depends_on, all_results)
                    if prior_context:
                        question = (
                            f"{step.context_hint}\n\n"
                            f"Context from prior queries:\n{prior_context}\n\n"
                            f"Question: {step.sub_question}"
                        )
                enriched.append(QueryStep(
                    id=step.id,
                    sub_question=question,
                    depends_on=step.depends_on,
                    context_hint=step.context_hint,
                    parallel_group=step.parallel_group,
                ))

            if len(enriched) == 1:
                result = await self._execute_step(enriched[0])
                all_results[enriched[0].id] = result
            else:
                tasks = [self._execute_step(s) for s in enriched]
                group_results = await asyncio.gather(*tasks)
                for step, result in zip(enriched, group_results):
                    all_results[step.id] = result

        # Return in original step order
        return [all_results[step.id] for step in plan.steps]

    async def _execute_step(self, step: QueryStep) -> StepResult:
        """Execute a single step."""
        try:
            response = await self._chat(step.sub_question)
            return StepResult(step=step, response=response)
        except Exception as exc:
            logger.error("Step %s failed: %s", step.id, exc)
            return StepResult(step=step, response=None, error=str(exc))

    def _build_context(
        self, depends_on: list[str], results: dict[str, StepResult]
    ) -> str:
        """Summarize results from prior steps for injection into a sub-question.

        Uses TOONS encoding for any structured data (statistics, etc.)
        to minimize token usage.
        """
        parts: list[str] = []
        for dep_id in depends_on:
            sr = results.get(dep_id)
            if not sr or not sr.response:
                continue
            r = sr.response
            parts.append(f"[{dep_id}] {sr.step.sub_question}")
            if r.sql_executed:
                parts.append(f"  SQL: {r.sql_executed}")
            parts.append(f"  Answer: {r.text[:500]}")
            if r.statistics:
                stats_toons = toons_encode(
                    {"statistics": r.statistics},
                    ToonsConfig(include_stats=True, include_suggestions=False),
                )
                parts.append(f"  {stats_toons}")
            parts.append("")
        return "\n".join(parts)


# ─── Synthesizer ─────────────────────────────────────────────────────────

_SYNTHESIS_PROMPT = """\
You are a data analyst. Combine the following sub-query results into a
single coherent answer to the user's original question.

## Original Question

{question}

## Plan Reasoning

{reasoning}

## Sub-Query Results

{results_text}

## Instructions

1. Lead with a one-sentence answer to the original question.
2. Reference specific numbers and data from the results.
3. Highlight 2-3 key statistics or insights.
4. If charts were generated in any step, mention them.
5. If any sub-queries failed, explain what information is missing.
6. Be concise — combine, don't just concatenate."""


class Synthesizer:
    """Merges multiple sub-query results into one answer.

    Args:
        llm: UnifiedLLM instance (can be a cheap/fast model).
    """

    def __init__(self, llm: UnifiedLLM):
        self._llm = llm

    async def synthesize(
        self,
        question: str,
        plan: QueryPlan,
        results: list[StepResult],
    ) -> str:
        """Merge step results into a single answer text.

        If the plan has only one step, returns that step's text directly
        (no extra LLM call).
        """
        if plan.is_simple and results and results[0].response:
            return results[0].response.text

        results_text = self._format_results(results)

        prompt = _SYNTHESIS_PROMPT.format(
            question=question,
            reasoning=plan.reasoning,
            results_text=results_text,
        )

        answer = await self._llm.complete(
            prompt,
            system="You are a data analyst synthesizing multiple query results.",
        )

        logger.info("Synthesized answer from %d sub-results", len(results))
        return answer

    def _format_results(self, results: list[StepResult]) -> str:
        """Format sub-results for the synthesis prompt.

        Uses TOONS for statistics and error data to minimize token usage.
        """
        parts: list[str] = []
        for sr in results:
            parts.append(f"### {sr.step.id}: {sr.step.sub_question}")
            if sr.error:
                parts.append(toons_encode(
                    {"error": {"message": sr.error}},
                ))
            elif sr.response:
                r = sr.response
                parts.append(f"SQL: {r.sql_executed or 'N/A'}")
                parts.append(f"Answer: {r.text}")
                if r.statistics:
                    parts.append(toons_encode(
                        {"statistics": r.statistics},
                        ToonsConfig(include_stats=True, include_suggestions=False),
                    ))
                if r.charts:
                    parts.append(f"Charts generated: {len(r.charts)}")
                if r.errors:
                    err_toons = toons_encode({"error": {"message": f"{len(r.errors)} errors"}})
                    parts.append(err_toons)
            parts.append("")
        return "\n".join(parts)


# ─── Heuristic: does this question need planning? ────────────────────────

_COMPLEX_PATTERNS = [
    r"\bcompare\b", r"\bvs\.?\b", r"\band also\b", r"\bthen show\b",
    r"\bbreak.*down\b", r"\bhow does.*compare\b", r"\bwhich.*and.*which\b",
    r"\btop.*bottom\b", r"\bmonth.over.month\b", r"\btrend.*by\b",
    r"\bcorrelat", r"\bbefore and after\b", r"\bimpact of\b",
    r"\bfirst.*then\b", r"\bfollow.?up\b", r"\badditionally\b",
]


def needs_planning(question: str) -> bool:
    """Heuristic: does this question likely need multiple queries?

    Returns True for complex multi-part questions. The agent can
    use this to decide whether to invoke the planner or go direct.
    """
    q_lower = question.lower()
    return any(re.search(p, q_lower) for p in _COMPLEX_PATTERNS)
