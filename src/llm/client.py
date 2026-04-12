"""LLM client abstraction — supports Anthropic and Ollama (OpenAI-compatible) backends."""
import base64
import os
import re
from typing import Protocol, runtime_checkable

import httpx


@runtime_checkable
class LLMClient(Protocol):
    def complete(self, system: str, messages: list[dict], max_tokens: int, response_schema: dict | None = None) -> str:
        """Send a chat request and return the response text."""
        ...


def _convert_messages(messages: list[dict]) -> list[dict]:
    """Convert Anthropic-format messages to OpenAI-format.

    Handles:
    - Plain string content → unchanged
    - List content blocks: image blocks converted from Anthropic source format
      to OpenAI image_url format; text blocks passed through unchanged.
    """
    result = []
    for msg in messages:
        role = msg["role"]
        content = msg["content"]

        if isinstance(content, str):
            result.append({"role": role, "content": content})
            continue

        # List of content blocks
        openai_blocks = []
        for block in content:
            if block.get("type") == "image":
                source = block.get("source", {})
                if source.get("type") == "base64":
                    media_type = source.get("media_type", "image/png")
                    data = source.get("data", "")
                    url = f"data:{media_type};base64,{data}"
                else:
                    url = source.get("url", "")
                openai_blocks.append({
                    "type": "image_url",
                    "image_url": {"url": url},
                })
            elif block.get("type") == "text":
                openai_blocks.append({"type": "text", "text": block["text"]})
            else:
                # Pass unknown blocks through unchanged
                openai_blocks.append(block)

        result.append({"role": role, "content": openai_blocks})

    return result


class AnthropicLLMClient:
    def __init__(self, model: str, anthropic_client):
        self._model = model
        self._client = anthropic_client

    def complete(self, system: str, messages: list[dict], max_tokens: int, response_schema: dict | None = None) -> str:
        resp = self._client.messages.create(
            model=self._model,
            max_tokens=max_tokens,
            system=system,
            messages=messages,
        )
        return resp.content[0].text.strip()


def _url_to_base64_data_uri(url: str) -> str:
    """Download an image URL and return a base64 data URI (required by Ollama)."""
    response = httpx.get(url, follow_redirects=True, timeout=30)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "image/png").split(";")[0].strip()
    b64 = base64.b64encode(response.content).decode()
    return f"data:{content_type};base64,{b64}"


def _inline_image_urls(messages: list[dict]) -> list[dict]:
    """Replace image_url URLs with base64 data URIs (Ollama requires base64)."""
    result = []
    for msg in messages:
        content = msg["content"]
        if not isinstance(content, list):
            result.append(msg)
            continue

        inlined = []
        for block in content:
            if block.get("type") == "image_url":
                url = block["image_url"]["url"]
                if not url.startswith("data:"):
                    url = _url_to_base64_data_uri(url)
                inlined.append({"type": "image_url", "image_url": {"url": url}})
            else:
                inlined.append(block)
        result.append({**msg, "content": inlined})

    return result


class OllamaLLMClient:
    def __init__(
        self,
        model: str,
        base_url: str = "http://localhost:11434/v1",
        num_ctx: int = 32768,
        api_key: str = "ollama",
    ):
        from openai import OpenAI
        self._client = OpenAI(base_url=base_url, api_key=api_key)
        self._model = model
        self._num_ctx = num_ctx

    def complete(self, system: str, messages: list[dict], max_tokens: int, response_schema: dict | None = None) -> str:
        openai_msgs = [{"role": "system", "content": system}] + _convert_messages(messages)
        openai_msgs = _inline_image_urls(openai_msgs)
        kwargs: dict = {
            "model": self._model,
            "messages": openai_msgs,
            "extra_body": {"options": {"num_ctx": self._num_ctx}},
        }
        if response_schema:
            kwargs["response_format"] = {"type": "json_object"}

        _debug = os.getenv("DEBUG_LLM")
        if _debug:
            print(f"[Ollama] model={self._model} num_ctx={self._num_ctx} "
                  f"msgs={len(openai_msgs)} response_format={kwargs.get('response_format')}")

        resp = self._client.chat.completions.create(**kwargs)

        choice = resp.choices[0]
        raw = choice.message.content or ""

        if _debug:
            print(f"[Ollama] finish_reason={choice.finish_reason!r} chars={len(raw)}")

        thinking = re.findall(r"<think>(.*?)</think>", raw, flags=re.DOTALL)
        text = re.sub(r"<think>.*?</think>\s*", "", raw, flags=re.DOTALL).strip()

        if _debug and thinking:
            print(f"[Ollama] stripped {sum(len(t) for t in thinking)} thinking chars")
        if not text:
            print("[Ollama WARNING] empty response after stripping thinking tokens")

        return text


def make_spec_client(
    backend: str,
    anthropic_client=None,
    ollama_model: str = "qwen3.5:4b",
    ollama_base_url: str = "http://localhost:11434/v1",
    ollama_api_key: str = "ollama",
) -> LLMClient:
    """Return the LLM client for spec extraction."""
    if backend == "ollama":
        return OllamaLLMClient(model=ollama_model, base_url=ollama_base_url, num_ctx=32768, api_key=ollama_api_key)
    return AnthropicLLMClient(model="claude-haiku-4-5-20251001", anthropic_client=anthropic_client)


def make_vision_client(
    backend: str,
    anthropic_client=None,
    ollama_model: str = "qwen3.5:4b",
    ollama_base_url: str = "http://localhost:11434/v1",
    ollama_api_key: str = "ollama",
) -> LLMClient:
    """Return the LLM client for vision/UI extraction."""
    if backend == "ollama":
        return OllamaLLMClient(model=ollama_model, base_url=ollama_base_url, num_ctx=32768, api_key=ollama_api_key)
    return AnthropicLLMClient(model="claude-sonnet-4-6", anthropic_client=anthropic_client)
