"""Unit tests for src/llm/client.py — _convert_messages() and client wrappers."""
import json
import pytest
from unittest.mock import MagicMock, patch

from src.llm.client import _convert_messages, _inline_image_urls, AnthropicLLMClient, OllamaLLMClient


# --- _convert_messages ---

def test_plain_string_content_passes_through():
    messages = [{"role": "user", "content": "Hello"}]
    result = _convert_messages(messages)
    assert result == [{"role": "user", "content": "Hello"}]


def test_text_block_passes_through():
    messages = [{"role": "user", "content": [{"type": "text", "text": "Describe this."}]}]
    result = _convert_messages(messages)
    assert result == [{"role": "user", "content": [{"type": "text", "text": "Describe this."}]}]


def test_anthropic_image_block_converts_to_openai_format():
    messages = [{
        "role": "user",
        "content": [
            {"type": "image", "source": {"type": "url", "url": "https://example.com/img.png"}},
        ],
    }]
    result = _convert_messages(messages)
    assert result == [{
        "role": "user",
        "content": [
            {"type": "image_url", "image_url": {"url": "https://example.com/img.png"}},
        ],
    }]


def test_multimodal_message_converts_image_preserves_text():
    messages = [{
        "role": "user",
        "content": [
            {"type": "image", "source": {"type": "url", "url": "https://example.com/ui.png"}},
            {"type": "text", "text": "Analyze this image."},
        ],
    }]
    result = _convert_messages(messages)
    assert result[0]["content"] == [
        {"type": "image_url", "image_url": {"url": "https://example.com/ui.png"}},
        {"type": "text", "text": "Analyze this image."},
    ]


def test_multiple_messages_all_converted():
    messages = [
        {"role": "user", "content": "First"},
        {"role": "assistant", "content": "Second"},
    ]
    result = _convert_messages(messages)
    assert len(result) == 2
    assert result[0]["content"] == "First"
    assert result[1]["content"] == "Second"


# --- AnthropicLLMClient ---

def test_anthropic_client_complete_returns_text():
    anthropic = MagicMock()
    content_block = MagicMock()
    content_block.text = '{"ok": true}'
    anthropic.messages.create.return_value = MagicMock(content=[content_block])

    client = AnthropicLLMClient(model="claude-haiku-4-5-20251001", anthropic_client=anthropic)
    result = client.complete(system="sys", messages=[{"role": "user", "content": "hi"}], max_tokens=100)

    assert result == '{"ok": true}'
    anthropic.messages.create.assert_called_once_with(
        model="claude-haiku-4-5-20251001",
        max_tokens=100,
        system="sys",
        messages=[{"role": "user", "content": "hi"}],
    )


# --- OllamaLLMClient ---

def test_ollama_client_strips_thinking_tokens():
    openai_mock = MagicMock()
    choice = MagicMock()
    choice.message.content = "<think>Let me think...</think>\n{\"result\": 42}"
    openai_mock.chat.completions.create.return_value = MagicMock(choices=[choice])

    with patch("src.llm.client.OllamaLLMClient.__init__", lambda self, model, base_url="http://localhost:11434/v1", num_ctx=32768: None):
        client = OllamaLLMClient.__new__(OllamaLLMClient)
        client._model = "qwen3.5:4b"
        client._num_ctx = 32768
        client._client = openai_mock

    result = client.complete(system="sys", messages=[{"role": "user", "content": "hi"}], max_tokens=100)
    assert result == '{"result": 42}'


def test_inline_image_urls_already_base64_passes_through():
    messages = [{"role": "user", "content": [
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc123"}},
    ]}]
    result = _inline_image_urls(messages)
    assert result[0]["content"][0]["image_url"]["url"] == "data:image/png;base64,abc123"


def test_inline_image_urls_downloads_http_url(monkeypatch):
    import httpx
    fake_response = MagicMock()
    fake_response.content = b"\x89PNG"
    fake_response.headers = {"content-type": "image/png"}
    monkeypatch.setattr(httpx, "get", lambda *a, **kw: fake_response)

    messages = [{"role": "user", "content": [
        {"type": "image_url", "image_url": {"url": "https://example.com/img.png"}},
    ]}]
    result = _inline_image_urls(messages)
    url = result[0]["content"][0]["image_url"]["url"]
    assert url.startswith("data:image/png;base64,")


def test_ollama_client_converts_image_blocks(monkeypatch):
    import httpx
    fake_response = MagicMock()
    fake_response.content = b"\x89PNG"
    fake_response.headers = {"content-type": "image/png"}
    monkeypatch.setattr(httpx, "get", lambda *a, **kw: fake_response)

    openai_mock = MagicMock()
    choice = MagicMock()
    choice.message.content = '{"is_ui_diagram": false}'
    openai_mock.chat.completions.create.return_value = MagicMock(choices=[choice])

    client = OllamaLLMClient.__new__(OllamaLLMClient)
    client._model = "qwen3.5:4b"
    client._num_ctx = 32768
    client._client = openai_mock

    messages = [{
        "role": "user",
        "content": [
            {"type": "image", "source": {"type": "url", "url": "https://example.com/img.png"}},
            {"type": "text", "text": "Analyze."},
        ],
    }]
    client.complete(system="sys", messages=messages, max_tokens=100)

    call_messages = openai_mock.chat.completions.create.call_args.kwargs["messages"]
    user_msg = call_messages[1]  # index 0 is system
    # URL should now be a base64 data URI, not the original URL
    assert user_msg["content"][0]["type"] == "image_url"
    assert user_msg["content"][0]["image_url"]["url"].startswith("data:image/png;base64,")
    assert user_msg["content"][1] == {"type": "text", "text": "Analyze."}


# --- num_ctx propagation ---

def test_ollama_client_passes_num_ctx_in_extra_body():
    openai_mock = MagicMock()
    choice = MagicMock()
    choice.message.content = '{"ok": true}'
    openai_mock.chat.completions.create.return_value = MagicMock(choices=[choice])

    client = OllamaLLMClient.__new__(OllamaLLMClient)
    client._model = "qwen3.5:4b"
    client._num_ctx = 32768
    client._client = openai_mock

    client.complete(system="sys", messages=[{"role": "user", "content": "hi"}], max_tokens=100)

    call_kwargs = openai_mock.chat.completions.create.call_args.kwargs
    assert call_kwargs["extra_body"] == {"options": {"num_ctx": 32768}}


def test_ollama_client_custom_num_ctx():
    openai_mock = MagicMock()
    choice = MagicMock()
    choice.message.content = '{"ok": true}'
    openai_mock.chat.completions.create.return_value = MagicMock(choices=[choice])

    client = OllamaLLMClient.__new__(OllamaLLMClient)
    client._model = "qwen3.5:4b"
    client._num_ctx = 8192
    client._client = openai_mock

    client.complete(system="sys", messages=[{"role": "user", "content": "hi"}], max_tokens=100)

    call_kwargs = openai_mock.chat.completions.create.call_args.kwargs
    assert call_kwargs["extra_body"] == {"options": {"num_ctx": 8192}}
