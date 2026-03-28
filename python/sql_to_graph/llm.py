"""LLM provider protocol and implementations for SQL auto-correction."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class LLMProvider(Protocol):
    """Protocol for LLM providers used in SQL auto-correction.

    Implement this protocol with any LLM backend. The only requirement
    is an async `complete` method that takes a prompt and returns text.
    """

    async def complete(self, prompt: str, system: str | None = None) -> str: ...


class OpenAIProvider:
    """LLM provider using OpenAI's API."""

    def __init__(self, model: str = "gpt-4o", api_key: str | None = None):
        self.model = model
        self.api_key = api_key
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from openai import AsyncOpenAI
            except ImportError:
                raise ImportError(
                    "openai package required. Install with: uv pip install 'sql_to_graph[llm]'"
                )
            self._client = AsyncOpenAI(api_key=self.api_key)
        return self._client

    async def complete(self, prompt: str, system: str | None = None) -> str:
        client = self._get_client()
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        response = await client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0,
        )
        return response.choices[0].message.content or ""


class AnthropicProvider:
    """LLM provider using Anthropic's API."""

    def __init__(self, model: str = "claude-sonnet-4-20250514", api_key: str | None = None):
        self.model = model
        self.api_key = api_key
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from anthropic import AsyncAnthropic
            except ImportError:
                raise ImportError(
                    "anthropic package required. Install with: uv pip install 'sql_to_graph[llm]'"
                )
            self._client = AsyncAnthropic(api_key=self.api_key)
        return self._client

    async def complete(self, prompt: str, system: str | None = None) -> str:
        client = self._get_client()
        kwargs = {
            "model": self.model,
            "max_tokens": 2048,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            kwargs["system"] = system

        response = await client.messages.create(**kwargs)
        return response.content[0].text


class LangChainProvider:
    """LLM provider wrapping any LangChain BaseChatModel or BaseLLM.

    Works with any LangChain chat model (ChatOpenAI, ChatAnthropic,
    ChatOllama, etc.) or base LLM.

    Usage:
        from langchain_openai import ChatOpenAI
        llm = ChatOpenAI(model="gpt-4o")
        provider = LangChainProvider(llm)
    """

    def __init__(self, llm: Any):
        self._llm = llm

    async def complete(self, prompt: str, system: str | None = None) -> str:
        try:
            from langchain_core.messages import HumanMessage, SystemMessage
        except ImportError:
            raise ImportError(
                "langchain-core package required. Install with: uv pip install langchain-core"
            )

        messages = []
        if system:
            messages.append(SystemMessage(content=system))
        messages.append(HumanMessage(content=prompt))

        response = await self._llm.ainvoke(messages)

        # Handle both ChatModel (returns BaseMessage) and LLM (returns str)
        if isinstance(response, str):
            return response
        return response.content
