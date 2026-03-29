"""Tests for LLM factory: UnifiedLLM protocol, create_llm, from_legacy."""

from unittest.mock import AsyncMock, MagicMock

import pytest

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


# ─── Factory tests ──────────────────────────────────────────────────────

def test_create_llm_anthropic():
    llm = create_llm("anthropic", model="claude-sonnet-4-20250514")
    assert isinstance(llm, AnthropicLLM)
    assert llm.model == "claude-sonnet-4-20250514"


def test_create_llm_openai():
    llm = create_llm("openai", model="gpt-4o")
    assert isinstance(llm, OpenAILLM)
    assert llm.model == "gpt-4o"


def test_create_llm_langchain():
    mock_chat = MagicMock()
    llm = create_llm("langchain", llm=mock_chat)
    assert isinstance(llm, LangChainLLM)


def test_create_llm_langchain_requires_llm():
    with pytest.raises(ValueError, match="llm parameter is required"):
        create_llm("langchain")


def test_create_llm_unknown_provider():
    with pytest.raises(ValueError, match="Unknown provider"):
        create_llm("gemini")


def test_create_llm_with_client():
    mock_client = MagicMock()
    llm = create_llm("anthropic", model="claude-sonnet-4-20250514", client=mock_client)
    assert isinstance(llm, AnthropicLLM)
    assert llm._client is mock_client


def test_create_llm_default_models():
    llm_a = create_llm("anthropic")
    assert llm_a.model == "claude-sonnet-4-20250514"
    llm_o = create_llm("openai")
    assert llm_o.model == "gpt-4o"


# ─── from_legacy tests ──────────────────────────────────────────────────

def test_from_legacy_anthropic():
    mock_client = MagicMock()
    llm = from_legacy(mock_client, "claude-sonnet-4-20250514", "anthropic")
    assert isinstance(llm, AnthropicLLM)
    assert llm._client is mock_client


def test_from_legacy_openai():
    mock_client = MagicMock()
    llm = from_legacy(mock_client, "gpt-4o", "openai")
    assert isinstance(llm, OpenAILLM)
    assert llm._client is mock_client


def test_from_legacy_passthrough():
    """If client already implements UnifiedLLM, pass through."""
    llm = create_llm("anthropic", model="test")
    result = from_legacy(llm, "test", "anthropic")
    assert result is llm


def test_from_legacy_unknown():
    with pytest.raises(ValueError, match="Unknown provider_type"):
        from_legacy(MagicMock(), "model", "gemini")


# ─── AnthropicLLM tests ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_anthropic_complete():
    from tests.conftest import _TextBlock, _AnthropicResponse

    mock_client = MagicMock()
    mock_client.messages = MagicMock()
    mock_client.messages.create = AsyncMock(
        return_value=_AnthropicResponse(content=[_TextBlock(text="hello")])
    )

    llm = AnthropicLLM(model="test", client=mock_client)
    result = await llm.complete("say hello")
    assert result == "hello"
    mock_client.messages.create.assert_called_once()


@pytest.mark.asyncio
async def test_anthropic_chat_with_tools():
    from tests.conftest import _ToolUseBlock, _TextBlock, _AnthropicResponse

    mock_client = MagicMock()
    mock_client.messages = MagicMock()
    mock_client.messages.create = AsyncMock(
        return_value=_AnthropicResponse(content=[
            _TextBlock(text="Let me query that"),
            _ToolUseBlock(id="tu_1", name="sql_to_graph", input={"sql": "SELECT 1"}),
        ])
    )

    llm = AnthropicLLM(model="test", client=mock_client)
    result = await llm.chat_with_tools(
        messages=[{"role": "user", "content": "test"}],
        tools=[{"name": "sql_to_graph", "description": "test", "input_schema": {}}],
    )

    assert isinstance(result, ChatWithToolsResult)
    assert len(result.text_parts) == 1
    assert len(result.tool_calls) == 1
    assert result.tool_calls[0].name == "sql_to_graph"
    assert result.has_tool_calls


def test_anthropic_format_tool_results():
    llm = AnthropicLLM(model="test")
    msg = llm.format_tool_results([
        ToolResultMessage(tool_call_id="tu_1", content='{"result": 42}'),
    ])
    assert msg["role"] == "user"
    assert msg["content"][0]["type"] == "tool_result"
    assert msg["content"][0]["tool_use_id"] == "tu_1"


# ─── OpenAILLM tests ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_openai_complete():
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "hello"
    mock_client.chat = MagicMock()
    mock_client.chat.completions = MagicMock()
    mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

    llm = OpenAILLM(model="test", client=mock_client)
    result = await llm.complete("say hello")
    assert result == "hello"


def test_openai_convert_tools():
    llm = OpenAILLM(model="test")
    anthropic_tools = [
        {"name": "sql_to_graph", "description": "Execute SQL", "input_schema": {"type": "object"}},
    ]
    openai_tools = llm._convert_tools(anthropic_tools)
    assert openai_tools[0]["type"] == "function"
    assert openai_tools[0]["function"]["name"] == "sql_to_graph"
    assert openai_tools[0]["function"]["parameters"] == {"type": "object"}


def test_openai_format_tool_results():
    llm = OpenAILLM(model="test")
    msg = llm.format_tool_results([
        ToolResultMessage(tool_call_id="tc_1", content='{"x": 1}'),
        ToolResultMessage(tool_call_id="tc_2", content='{"y": 2}'),
    ])
    assert msg["role"] == "_multi"
    assert len(msg["messages"]) == 2
    assert msg["messages"][0]["role"] == "tool"


# ─── ChatWithToolsResult tests ──────────────────────────────────────────

def test_chat_result_text():
    r = ChatWithToolsResult(text_parts=["hello", "world"])
    assert r.text == "hello\nworld"


def test_chat_result_has_tool_calls():
    r1 = ChatWithToolsResult(tool_calls=[ToolCallRequest(id="1", name="t", arguments={})])
    assert r1.has_tool_calls
    r2 = ChatWithToolsResult()
    assert not r2.has_tool_calls
