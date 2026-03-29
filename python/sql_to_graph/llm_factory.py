"""Universal LLM abstraction with factory function.

Supports Anthropic, OpenAI, and LangChain (default) backends through a
unified interface for both simple completions and tool-calling loops.

Usage::

    from sql_to_graph.llm_factory import create_llm

    # LangChain (default)
    from langchain_anthropic import ChatAnthropic
    llm = create_llm("langchain", llm=ChatAnthropic(model="claude-sonnet-4-20250514"))

    # Anthropic direct
    llm = create_llm("anthropic", model="claude-sonnet-4-20250514")

    # OpenAI direct
    llm = create_llm("openai", model="gpt-4o")

    # Use in the agent
    agent = DataAnalystAgent(connection_string="...", llm=llm)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

logger = logging.getLogger("sql_to_graph.llm_factory")


# ─── Canonical types ─────────────────────────────────────────────────────

@dataclass
class ToolCallRequest:
    """A single tool call requested by the LLM."""

    id: str
    name: str
    arguments: dict[str, Any]


@dataclass
class ChatWithToolsResult:
    """Normalized result from a tool-calling LLM turn."""

    text_parts: list[str] = field(default_factory=list)
    tool_calls: list[ToolCallRequest] = field(default_factory=list)
    raw_response: Any = None

    @property
    def has_tool_calls(self) -> bool:
        return bool(self.tool_calls)

    @property
    def text(self) -> str:
        return "\n".join(self.text_parts)


@dataclass
class ToolResultMessage:
    """A tool result to send back to the LLM."""

    tool_call_id: str
    content: str


# ─── Protocol ────────────────────────────────────────────────────────────

@runtime_checkable
class UnifiedLLM(Protocol):
    """Protocol for LLMs that support both completion and tool calling.

    Implement this protocol to plug any LLM backend into the agent.
    """

    async def complete(
        self, prompt: str, system: str | None = None
    ) -> str:
        """Simple text completion. Used by planner, reflector, synthesizer."""
        ...

    async def chat_with_tools(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        system: str | None = None,
        max_tokens: int = 4096,
    ) -> ChatWithToolsResult:
        """Single turn of tool-calling chat. Returns normalized result.

        Messages use a canonical format:
          - {"role": "user", "content": "..."}
          - {"role": "assistant", "content": "...", "tool_calls": [ToolCallRequest(...)]}
          - {"role": "tool_result", "tool_call_id": "...", "content": "..."}

        Tools are in Anthropic format (name, description, input_schema).
        Implementations translate to their native format internally.
        """
        ...

    def format_tool_results(
        self,
        results: list[ToolResultMessage],
    ) -> dict[str, Any]:
        """Format tool results as a message for the next turn.

        Returns a dict suitable for appending to the messages list.
        """
        ...

    def format_assistant_message(
        self,
        result: ChatWithToolsResult,
    ) -> dict[str, Any]:
        """Format the assistant's response as a message for history.

        Returns a dict suitable for appending to the messages list.
        """
        ...


# ─── Anthropic implementation ────────────────────────────────────────────

class AnthropicLLM:
    """UnifiedLLM backed by Anthropic's API (AsyncAnthropic).

    Args:
        model: Model name (e.g. "claude-sonnet-4-20250514").
        api_key: Optional API key. Defaults to ANTHROPIC_API_KEY env var.
        client: Optional pre-built AsyncAnthropic client.
    """

    def __init__(
        self,
        model: str = "claude-sonnet-4-20250514",
        api_key: str | None = None,
        client: Any | None = None,
    ):
        self.model = model
        self._api_key = api_key
        self._client = client

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                from anthropic import AsyncAnthropic
            except ImportError:
                raise ImportError(
                    "anthropic package required. Install: pip install 'sql-to-graph[llm]'"
                )
            self._client = AsyncAnthropic(api_key=self._api_key)
        return self._client

    async def complete(self, prompt: str, system: str | None = None) -> str:
        client = self._get_client()
        kwargs: dict[str, Any] = {
            "model": self.model,
            "max_tokens": 4096,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            kwargs["system"] = system
        response = await client.messages.create(**kwargs)
        return response.content[0].text

    async def chat_with_tools(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        system: str | None = None,
        max_tokens: int = 4096,
    ) -> ChatWithToolsResult:
        client = self._get_client()
        # Convert canonical messages to Anthropic format
        api_messages = self._to_anthropic_messages(messages)

        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": api_messages,
            "tools": tools,  # already in Anthropic format
            "max_tokens": max_tokens,
        }
        if system:
            kwargs["system"] = system

        response = await client.messages.create(**kwargs)

        text_parts: list[str] = []
        tool_calls: list[ToolCallRequest] = []
        for block in response.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                tool_calls.append(ToolCallRequest(
                    id=block.id,
                    name=block.name,
                    arguments=block.input,
                ))

        return ChatWithToolsResult(
            text_parts=text_parts,
            tool_calls=tool_calls,
            raw_response=response,
        )

    def format_tool_results(self, results: list[ToolResultMessage]) -> dict[str, Any]:
        return {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": r.tool_call_id,
                    "content": r.content,
                }
                for r in results
            ],
        }

    def format_assistant_message(self, result: ChatWithToolsResult) -> dict[str, Any]:
        # Anthropic expects the raw content blocks
        return {"role": "assistant", "content": result.raw_response.content}

    def _to_anthropic_messages(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert canonical messages to Anthropic API format."""
        api_msgs: list[dict[str, Any]] = []
        for msg in messages:
            role = msg["role"]
            if role == "user":
                api_msgs.append({"role": "user", "content": msg["content"]})
            elif role == "assistant":
                # If it has raw_content (from format_assistant_message), use it
                if "raw_content" in msg:
                    api_msgs.append({"role": "assistant", "content": msg["raw_content"]})
                else:
                    api_msgs.append({"role": "assistant", "content": msg.get("content", "")})
            elif role == "tool_result":
                # Batch tool results into a user message
                if api_msgs and api_msgs[-1].get("role") == "user" and isinstance(api_msgs[-1].get("content"), list):
                    api_msgs[-1]["content"].append({
                        "type": "tool_result",
                        "tool_use_id": msg["tool_call_id"],
                        "content": msg["content"],
                    })
                else:
                    api_msgs.append({
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": msg["tool_call_id"],
                            "content": msg["content"],
                        }],
                    })
        return api_msgs


# ─── OpenAI implementation ───────────────────────────────────────────────

class OpenAILLM:
    """UnifiedLLM backed by OpenAI's API (AsyncOpenAI).

    Args:
        model: Model name (e.g. "gpt-4o").
        api_key: Optional API key. Defaults to OPENAI_API_KEY env var.
        client: Optional pre-built AsyncOpenAI client.
    """

    def __init__(
        self,
        model: str = "gpt-4o",
        api_key: str | None = None,
        client: Any | None = None,
    ):
        self.model = model
        self._api_key = api_key
        self._client = client

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                from openai import AsyncOpenAI
            except ImportError:
                raise ImportError(
                    "openai package required. Install: pip install 'sql-to-graph[llm]'"
                )
            self._client = AsyncOpenAI(api_key=self._api_key)
        return self._client

    async def complete(self, prompt: str, system: str | None = None) -> str:
        client = self._get_client()
        messages: list[dict[str, Any]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        response = await client.chat.completions.create(
            model=self.model, messages=messages, temperature=0,
        )
        return response.choices[0].message.content or ""

    async def chat_with_tools(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        system: str | None = None,
        max_tokens: int = 4096,
    ) -> ChatWithToolsResult:
        client = self._get_client()
        # Convert Anthropic-format tools to OpenAI format
        openai_tools = self._convert_tools(tools)
        api_messages = self._to_openai_messages(messages, system)

        response = await client.chat.completions.create(
            model=self.model,
            messages=api_messages,
            tools=openai_tools,
            temperature=0,
        )

        choice = response.choices[0]
        message = choice.message

        text_parts: list[str] = [message.content] if message.content else []
        tool_calls: list[ToolCallRequest] = []
        if message.tool_calls:
            for tc in message.tool_calls:
                tool_calls.append(ToolCallRequest(
                    id=tc.id,
                    name=tc.function.name,
                    arguments=json.loads(tc.function.arguments),
                ))

        return ChatWithToolsResult(
            text_parts=text_parts,
            tool_calls=tool_calls,
            raw_response=response,
        )

    def format_tool_results(self, results: list[ToolResultMessage]) -> dict[str, Any]:
        # OpenAI sends each tool result as a separate message; return list
        # We use a special key to signal multiple messages
        return {
            "role": "_multi",
            "messages": [
                {
                    "role": "tool",
                    "tool_call_id": r.tool_call_id,
                    "content": r.content,
                }
                for r in results
            ],
        }

    def format_assistant_message(self, result: ChatWithToolsResult) -> dict[str, Any]:
        # OpenAI expects the raw message dict
        return result.raw_response.choices[0].message.model_dump()

    def _convert_tools(self, tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert Anthropic-format tools to OpenAI function-calling format."""
        converted = []
        for t in tools:
            if "function" in t:
                # Already OpenAI format
                converted.append(t)
            else:
                converted.append({
                    "type": "function",
                    "function": {
                        "name": t["name"],
                        "description": t.get("description", ""),
                        "parameters": t.get("input_schema", {}),
                    },
                })
        return converted

    def _to_openai_messages(
        self, messages: list[dict[str, Any]], system: str | None = None
    ) -> list[dict[str, Any]]:
        """Convert canonical messages to OpenAI API format."""
        api_msgs: list[dict[str, Any]] = []
        if system:
            api_msgs.append({"role": "system", "content": system})

        for msg in messages:
            role = msg["role"]
            if role in ("user", "system"):
                api_msgs.append({"role": role, "content": msg["content"]})
            elif role == "assistant":
                api_msgs.append(msg if "tool_calls" not in msg else msg)
            elif role == "tool_result":
                api_msgs.append({
                    "role": "tool",
                    "tool_call_id": msg["tool_call_id"],
                    "content": msg["content"],
                })
            elif role == "_multi":
                api_msgs.extend(msg["messages"])
        return api_msgs


# ─── LangChain implementation ────────────────────────────────────────────

class LangChainLLM:
    """UnifiedLLM backed by any LangChain BaseChatModel (default).

    This is the recommended provider. It works with ChatAnthropic,
    ChatOpenAI, ChatOllama, ChatVertexAI, or any LangChain chat model.

    Args:
        llm: A LangChain BaseChatModel instance.
    """

    def __init__(self, llm: Any):
        self._llm = llm
        self._check_import()

    def _check_import(self) -> None:
        try:
            from langchain_core.messages import HumanMessage  # noqa: F401
        except ImportError:
            raise ImportError(
                "langchain-core required. Install: pip install 'sql-to-graph[langchain]'"
            )

    async def complete(self, prompt: str, system: str | None = None) -> str:
        from langchain_core.messages import HumanMessage, SystemMessage

        messages: list = []
        if system:
            messages.append(SystemMessage(content=system))
        messages.append(HumanMessage(content=prompt))
        response = await self._llm.ainvoke(messages)
        return response.content if hasattr(response, "content") else str(response)

    async def chat_with_tools(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        system: str | None = None,
        max_tokens: int = 4096,
    ) -> ChatWithToolsResult:
        from langchain_core.messages import (
            AIMessage,
            HumanMessage,
            SystemMessage,
            ToolMessage,
        )

        # Convert tools to LangChain format and bind
        lc_tools = self._convert_tools(tools)
        llm_with_tools = self._llm.bind_tools(lc_tools)

        # Convert canonical messages to LangChain messages
        lc_messages = self._to_langchain_messages(messages, system)

        response: AIMessage = await llm_with_tools.ainvoke(lc_messages)

        text_parts: list[str] = []
        if response.content:
            if isinstance(response.content, str):
                text_parts.append(response.content)
            elif isinstance(response.content, list):
                for block in response.content:
                    if isinstance(block, str):
                        text_parts.append(block)
                    elif isinstance(block, dict) and block.get("type") == "text":
                        text_parts.append(block["text"])

        tool_calls: list[ToolCallRequest] = []
        if hasattr(response, "tool_calls") and response.tool_calls:
            for tc in response.tool_calls:
                tool_calls.append(ToolCallRequest(
                    id=tc.get("id", ""),
                    name=tc["name"],
                    arguments=tc.get("args", {}),
                ))

        return ChatWithToolsResult(
            text_parts=text_parts,
            tool_calls=tool_calls,
            raw_response=response,
        )

    def format_tool_results(self, results: list[ToolResultMessage]) -> dict[str, Any]:
        # Return in canonical format — _to_langchain_messages will convert
        return {
            "role": "_tool_results",
            "results": [
                {"tool_call_id": r.tool_call_id, "content": r.content}
                for r in results
            ],
        }

    def format_assistant_message(self, result: ChatWithToolsResult) -> dict[str, Any]:
        return {"role": "assistant", "raw_lc_message": result.raw_response}

    def _convert_tools(self, tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert Anthropic-format tools to LangChain tool format."""
        lc_tools = []
        for t in tools:
            lc_tools.append({
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "parameters": t.get("input_schema", {}),
                },
            })
        return lc_tools

    def _to_langchain_messages(
        self, messages: list[dict[str, Any]], system: str | None = None
    ) -> list:
        from langchain_core.messages import (
            AIMessage,
            HumanMessage,
            SystemMessage,
            ToolMessage,
        )

        lc_msgs: list = []
        if system:
            lc_msgs.append(SystemMessage(content=system))

        for msg in messages:
            role = msg["role"]
            if role == "user":
                lc_msgs.append(HumanMessage(content=msg["content"]))
            elif role == "assistant":
                if "raw_lc_message" in msg:
                    lc_msgs.append(msg["raw_lc_message"])
                else:
                    lc_msgs.append(AIMessage(content=msg.get("content", "")))
            elif role == "tool_result":
                lc_msgs.append(ToolMessage(
                    content=msg["content"],
                    tool_call_id=msg["tool_call_id"],
                ))
            elif role == "_tool_results":
                for r in msg["results"]:
                    lc_msgs.append(ToolMessage(
                        content=r["content"],
                        tool_call_id=r["tool_call_id"],
                    ))
        return lc_msgs


# ─── Factory ─────────────────────────────────────────────────────────────

def create_llm(
    provider: Literal["langchain", "anthropic", "openai"] = "langchain",
    *,
    model: str | None = None,
    api_key: str | None = None,
    llm: Any | None = None,
    client: Any | None = None,
) -> UnifiedLLM:
    """Create a UnifiedLLM instance.

    Args:
        provider: Backend to use. Default is ``"langchain"``.
        model: Model name. Required for ``"anthropic"`` and ``"openai"``.
            Ignored for ``"langchain"`` (model is set on the chat model).
        api_key: Optional API key for ``"anthropic"`` or ``"openai"``.
        llm: A LangChain ``BaseChatModel`` instance. Required when
            ``provider="langchain"``.
        client: Optional pre-built async client for ``"anthropic"`` or
            ``"openai"`` providers.

    Returns:
        A :class:`UnifiedLLM` instance ready for use with :class:`DataAnalystAgent`.

    Examples::

        # LangChain (default) — works with any chat model
        from langchain_anthropic import ChatAnthropic
        llm = create_llm("langchain", llm=ChatAnthropic(model="claude-sonnet-4-20250514"))

        # Direct Anthropic
        llm = create_llm("anthropic", model="claude-sonnet-4-20250514")

        # Direct OpenAI
        llm = create_llm("openai", model="gpt-4o")

        # With pre-built client
        from anthropic import AsyncAnthropic
        llm = create_llm("anthropic", model="claude-sonnet-4-20250514",
                          client=AsyncAnthropic(api_key="sk-..."))
    """
    if provider == "langchain":
        if llm is None:
            raise ValueError(
                "llm parameter is required for langchain provider. "
                "Pass a LangChain BaseChatModel instance."
            )
        return LangChainLLM(llm)

    elif provider == "anthropic":
        return AnthropicLLM(
            model=model or "claude-sonnet-4-20250514",
            api_key=api_key,
            client=client,
        )

    elif provider == "openai":
        return OpenAILLM(
            model=model or "gpt-4o",
            api_key=api_key,
            client=client,
        )

    else:
        raise ValueError(f"Unknown provider: {provider!r}. Use 'langchain', 'anthropic', or 'openai'.")


def from_legacy(
    llm_client: Any,
    model: str,
    provider_type: Literal["anthropic", "openai"],
) -> UnifiedLLM:
    """Wrap a raw client + provider_type into UnifiedLLM for backward compatibility.

    This allows existing code using ``DataAnalystAgent(llm_client=..., provider_type=...)``
    to keep working unchanged.
    """
    if isinstance(llm_client, UnifiedLLM):
        return llm_client

    if provider_type == "anthropic":
        return AnthropicLLM(model=model, client=llm_client)
    elif provider_type == "openai":
        return OpenAILLM(model=model, client=llm_client)
    else:
        raise ValueError(f"Unknown provider_type: {provider_type!r}")
