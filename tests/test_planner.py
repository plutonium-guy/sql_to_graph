"""Tests for QueryPlanner, ParallelExecutor, and Synthesizer."""

import asyncio
import json
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock

import pytest

from sql_to_graph.planner import (
    ParallelExecutor,
    QueryPlan,
    QueryPlanner,
    QueryStep,
    StepResult,
    Synthesizer,
    needs_planning,
)


# ─── Fixtures ────────────────────────────────────────────────────────────

@dataclass
class FakeAgentResponse:
    text: str = ""
    rounds_used: int = 1
    charts: list = field(default_factory=list)
    statistics: dict = None
    sql_executed: str = None
    tool_calls: list = field(default_factory=list)
    errors: list = field(default_factory=list)


@pytest.fixture
def mock_planner_llm():
    llm = MagicMock()
    llm.complete = AsyncMock()
    return llm


# ─── needs_planning heuristic ────────────────────────────────────────────

def test_needs_planning_simple():
    assert not needs_planning("How many orders are there?")
    assert not needs_planning("Show me the top 10 customers")
    assert not needs_planning("What is the average order amount?")


def test_needs_planning_complex():
    assert needs_planning("Compare revenue by region vs by segment")
    assert needs_planning("Show me the trend by month and also break down by category")
    assert needs_planning("Which region has the highest revenue and which has the lowest?")
    assert needs_planning("What is the month-over-month growth?")
    assert needs_planning("First show me total sales, then break down by product")


# ─── QueryPlanner ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_planner_single_step(mock_planner_llm):
    mock_planner_llm.complete.return_value = json.dumps({
        "reasoning": "Simple count query",
        "steps": [
            {"id": "step_1", "sub_question": "How many orders are there?", "depends_on": []}
        ]
    })

    planner = QueryPlanner(mock_planner_llm)
    plan = await planner.plan("How many orders?", "-- Schema: ecommerce")

    assert plan.is_simple
    assert len(plan.steps) == 1
    assert plan.steps[0].sub_question == "How many orders are there?"


@pytest.mark.asyncio
async def test_planner_multi_step(mock_planner_llm):
    mock_planner_llm.complete.return_value = json.dumps({
        "reasoning": "Need two queries: totals and breakdown",
        "steps": [
            {"id": "step_1", "sub_question": "Total revenue by region", "depends_on": []},
            {"id": "step_2", "sub_question": "Top region details", "depends_on": ["step_1"],
             "context_hint": "Use the top region from step_1"},
        ]
    })

    planner = QueryPlanner(mock_planner_llm)
    plan = await planner.plan("Compare regions and show top region details", "-- Schema")

    assert not plan.is_simple
    assert len(plan.steps) == 2
    assert plan.steps[0].parallel_group == 0
    assert plan.steps[1].parallel_group == 1  # depends on step_1


@pytest.mark.asyncio
async def test_planner_parallel_groups(mock_planner_llm):
    mock_planner_llm.complete.return_value = json.dumps({
        "reasoning": "Two independent queries",
        "steps": [
            {"id": "step_1", "sub_question": "Revenue by region", "depends_on": []},
            {"id": "step_2", "sub_question": "Revenue by category", "depends_on": []},
            {"id": "step_3", "sub_question": "Combine both", "depends_on": ["step_1", "step_2"]},
        ]
    })

    planner = QueryPlanner(mock_planner_llm)
    plan = await planner.plan("Compare region and category revenue", "-- Schema")

    # step_1 and step_2 should be in the same group (parallel)
    assert plan.steps[0].parallel_group == plan.steps[1].parallel_group
    # step_3 should be in a later group
    assert plan.steps[2].parallel_group > plan.steps[0].parallel_group


@pytest.mark.asyncio
async def test_planner_handles_invalid_json(mock_planner_llm):
    mock_planner_llm.complete.return_value = "this is not json"

    planner = QueryPlanner(mock_planner_llm)
    plan = await planner.plan("How many orders?", "-- Schema")

    # Should fallback to single-step with original question
    assert plan.is_simple
    assert "How many orders?" in plan.steps[0].sub_question


@pytest.mark.asyncio
async def test_planner_handles_markdown_fenced_json(mock_planner_llm):
    mock_planner_llm.complete.return_value = """```json
{
    "reasoning": "Simple query",
    "steps": [{"id": "step_1", "sub_question": "Count orders", "depends_on": []}]
}
```"""

    planner = QueryPlanner(mock_planner_llm)
    plan = await planner.plan("Count orders", "-- Schema")

    assert plan.is_simple
    assert plan.steps[0].sub_question == "Count orders"


# ─── ParallelExecutor ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_executor_single_step():
    chat_fn = AsyncMock(return_value=FakeAgentResponse(text="1000 orders"))

    executor = ParallelExecutor(chat_fn)
    plan = QueryPlan(
        original_question="How many orders?",
        steps=[QueryStep(id="step_1", sub_question="How many orders?")],
    )

    results = await executor.execute(plan)
    assert len(results) == 1
    assert results[0].response.text == "1000 orders"
    assert results[0].error is None


@pytest.mark.asyncio
async def test_executor_parallel_steps():
    """Two independent steps should both execute."""
    call_order = []

    async def mock_chat(question):
        call_order.append(question)
        return FakeAgentResponse(text=f"Answer to: {question}")

    executor = ParallelExecutor(mock_chat)
    plan = QueryPlan(
        original_question="test",
        steps=[
            QueryStep(id="s1", sub_question="Query A", parallel_group=0),
            QueryStep(id="s2", sub_question="Query B", parallel_group=0),
        ],
    )

    results = await executor.execute(plan)
    assert len(results) == 2
    assert all(r.error is None for r in results)
    assert len(call_order) == 2


@pytest.mark.asyncio
async def test_executor_dependent_steps():
    """Steps with dependencies get context from prior results."""
    async def mock_chat(question):
        return FakeAgentResponse(
            text=f"Answer: {question[:50]}",
            sql_executed="SELECT 1",
        )

    executor = ParallelExecutor(mock_chat)
    plan = QueryPlan(
        original_question="test",
        steps=[
            QueryStep(id="s1", sub_question="Get top region", parallel_group=0),
            QueryStep(
                id="s2",
                sub_question="Details for top region",
                depends_on=["s1"],
                context_hint="Use the region from s1",
                parallel_group=1,
            ),
        ],
    )

    results = await executor.execute(plan)
    assert len(results) == 2
    assert results[0].step.id == "s1"
    assert results[1].step.id == "s2"


@pytest.mark.asyncio
async def test_executor_handles_step_failure():
    async def failing_chat(question):
        if "fail" in question:
            raise RuntimeError("Query failed")
        return FakeAgentResponse(text="OK")

    executor = ParallelExecutor(failing_chat)
    plan = QueryPlan(
        original_question="test",
        steps=[
            QueryStep(id="s1", sub_question="This should fail", parallel_group=0),
            QueryStep(id="s2", sub_question="This should work", parallel_group=0),
        ],
    )

    results = await executor.execute(plan)
    assert results[0].error is not None
    assert "failed" in results[0].error.lower()
    assert results[1].error is None


# ─── Synthesizer ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_synthesizer_single_step_passthrough():
    """Single-step plans skip synthesis (no extra LLM call)."""
    llm = MagicMock()
    llm.complete = AsyncMock()

    synth = Synthesizer(llm)
    plan = QueryPlan(
        original_question="How many orders?",
        steps=[QueryStep(id="s1", sub_question="How many orders?")],
    )
    results = [StepResult(
        step=plan.steps[0],
        response=FakeAgentResponse(text="There are 1000 orders."),
    )]

    answer = await synth.synthesize("How many orders?", plan, results)
    assert answer == "There are 1000 orders."
    llm.complete.assert_not_called()


@pytest.mark.asyncio
async def test_synthesizer_multi_step():
    llm = MagicMock()
    llm.complete = AsyncMock(return_value="East region has the highest revenue at $500K.")

    synth = Synthesizer(llm)
    plan = QueryPlan(
        original_question="Compare regions",
        steps=[
            QueryStep(id="s1", sub_question="Revenue by region"),
            QueryStep(id="s2", sub_question="Top region details"),
        ],
        reasoning="Need two queries",
    )
    results = [
        StepResult(step=plan.steps[0], response=FakeAgentResponse(
            text="East=$500K, West=$400K", sql_executed="SELECT region, SUM(amount) ..."
        )),
        StepResult(step=plan.steps[1], response=FakeAgentResponse(
            text="East region has 200 customers", sql_executed="SELECT COUNT(*) ..."
        )),
    ]

    answer = await synth.synthesize("Compare regions", plan, results)
    assert "500K" in answer
    llm.complete.assert_called_once()
