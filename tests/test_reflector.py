"""Tests for ReflectionAgent."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from sql_to_graph.reflector import ReflectionAgent, ReflectionResult


@pytest.fixture
def mock_reflector_llm():
    llm = MagicMock()
    llm.complete = AsyncMock()
    return llm


# ─── ReflectionResult ────────────────────────────────────────────────────

def test_reflection_result_accepted():
    r = ReflectionResult(accepted=True)
    assert r.accepted
    assert r.critique is None


def test_reflection_result_rejected():
    r = ReflectionResult(accepted=False, critique="Wrong table", retry_hint="Use orders not order_items")
    assert not r.accepted
    assert "Wrong table" in r.critique


# ─── ReflectionAgent.reflect ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_reflect_accepts(mock_reflector_llm):
    mock_reflector_llm.complete.return_value = json.dumps({
        "accepted": True,
        "critique": None,
        "retry_hint": None,
        "improved_text": None,
    })

    reflector = ReflectionAgent(mock_reflector_llm)
    result = await reflector.reflect(
        question="How many orders?",
        answer="There are 1000 orders.",
        sql_executed="SELECT COUNT(*) FROM orders",
    )

    assert result.accepted
    assert result.critique is None
    mock_reflector_llm.complete.assert_called_once()


@pytest.mark.asyncio
async def test_reflect_rejects(mock_reflector_llm):
    mock_reflector_llm.complete.return_value = json.dumps({
        "accepted": False,
        "critique": "The query counts all orders but user asked for active only",
        "retry_hint": "Add WHERE status = 'active'",
        "improved_text": None,
    })

    reflector = ReflectionAgent(mock_reflector_llm)
    result = await reflector.reflect(
        question="How many active orders?",
        answer="There are 1000 orders.",
        sql_executed="SELECT COUNT(*) FROM orders",
    )

    assert not result.accepted
    assert "active" in result.critique
    assert "WHERE" in result.retry_hint


@pytest.mark.asyncio
async def test_reflect_with_improved_text(mock_reflector_llm):
    mock_reflector_llm.complete.return_value = json.dumps({
        "accepted": True,
        "critique": None,
        "retry_hint": None,
        "improved_text": "There are 1000 orders in the database, averaging $50 each.",
    })

    reflector = ReflectionAgent(mock_reflector_llm)
    result = await reflector.reflect(
        question="How many orders?",
        answer="There are 1000 orders.",
    )

    assert result.accepted
    assert result.improved_text is not None
    assert "averaging" in result.improved_text


@pytest.mark.asyncio
async def test_reflect_handles_invalid_json(mock_reflector_llm):
    mock_reflector_llm.complete.return_value = "not valid json"

    reflector = ReflectionAgent(mock_reflector_llm)
    result = await reflector.reflect(
        question="test", answer="test",
    )

    # Should assume accepted on parse failure
    assert result.accepted


@pytest.mark.asyncio
async def test_reflect_handles_markdown_fenced_json(mock_reflector_llm):
    mock_reflector_llm.complete.return_value = """```json
{"accepted": false, "critique": "Missing filter", "retry_hint": "Add WHERE clause"}
```"""

    reflector = ReflectionAgent(mock_reflector_llm)
    result = await reflector.reflect(question="test", answer="test")

    assert not result.accepted
    assert "Missing filter" in result.critique


# ─── should_reflect heuristic ────────────────────────────────────────────

def test_should_reflect_with_errors(mock_reflector_llm):
    reflector = ReflectionAgent(mock_reflector_llm, skip_simple=True)
    assert reflector.should_reflect(
        rounds_used=1,
        errors=[{"error": "something"}],
    )


def test_should_not_reflect_simple_query(mock_reflector_llm):
    reflector = ReflectionAgent(mock_reflector_llm, skip_simple=True)
    assert not reflector.should_reflect(
        rounds_used=2,
        sql_executed="SELECT COUNT(*) FROM orders",
    )


def test_should_reflect_complex_query(mock_reflector_llm):
    reflector = ReflectionAgent(mock_reflector_llm, skip_simple=True)
    assert reflector.should_reflect(
        rounds_used=3,
        sql_executed="WITH cte AS (SELECT * FROM orders) SELECT * FROM cte JOIN customers ON ...",
    )


def test_should_reflect_join_query(mock_reflector_llm):
    reflector = ReflectionAgent(mock_reflector_llm, skip_simple=True)
    assert reflector.should_reflect(
        rounds_used=2,
        sql_executed="SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id",
    )


def test_should_reflect_skip_disabled(mock_reflector_llm):
    reflector = ReflectionAgent(mock_reflector_llm, skip_simple=False)
    # Even simple queries should reflect when skip_simple=False
    assert reflector.should_reflect(
        rounds_used=2,
        sql_executed="SELECT COUNT(*) FROM orders",
    )
