"""
Tests for SpecExtractor — verifies behavior through public interface only.
Unit tests use a fake Anthropic client that returns canned JSON responses.
Integration tests (marked @pytest.mark.integration) hit the live LLM.
"""
import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock
from datetime import datetime, timezone

from src.staging.models import RawPage

FIXTURES = Path(__file__).parent / "fixtures"


def make_raw_page(markdown: str, url: str = "https://sofirnlight.com/products/sc33") -> RawPage:
    return RawPage(
        id=1,
        crawl_run_id=1,
        url=url,
        markdown=markdown,
        image_urls=[],
        raw_variant_data=None,
        crawled_at=datetime.now(timezone.utc),
        scraper_version="test",
    )


def fake_anthropic_client(response_graph: dict):
    """Return a minimal fake Anthropic client that yields a canned ConfigurationGraph JSON."""
    content_block = MagicMock()
    content_block.text = json.dumps(response_graph)

    message = MagicMock()
    message.content = [content_block]

    client = MagicMock()
    client.messages.create.return_value = message
    return client


SC33_GRAPH = {
    "product_name": "Sofirn SC33",
    "brand": "sofirn",
    "leds": [{"name": "XHP70.3 HI", "cct_hints": ["6500K", "5000K"]}],
    "drivers": [{"name": "Boost driver"}],
    "pairings": [{"led": "XHP70.3 HI", "driver": "Boost driver"}],
    "specs": {
        "length_mm": 131,
        "weight_g": 110,
        "material": "AL6061-T6 aluminum alloy",
        "max_lumens": 5200,
    },
    "price": "31.99",
    "source_url": "https://sofirnlight.com/products/sc33",
}


# --- Behavior 1: Extracts product name and LED from SC33 fixture markdown ---

def test_extract_returns_product_name_and_led():
    from src.extractor.spec_extractor import SpecExtractor

    markdown = (FIXTURES / "sofirn_multi_variant_page.md").read_text(encoding="utf-8")
    raw_page = make_raw_page(markdown)
    client = fake_anthropic_client(SC33_GRAPH)

    extractor = SpecExtractor(client)
    result = extractor.extract(raw_page)

    assert "SC33" in result.configuration_graph["product_name"]
    leds = result.configuration_graph["leds"]
    assert len(leds) > 0
    assert any("XHP70" in led["name"] for led in leds)


# --- Behavior 2: Extracts physical specs from a spec table ---

def test_extract_returns_physical_specs():
    from src.extractor.spec_extractor import SpecExtractor

    markdown = (FIXTURES / "sofirn_multi_variant_page.md").read_text(encoding="utf-8")
    raw_page = make_raw_page(markdown)
    client = fake_anthropic_client(SC33_GRAPH)

    extractor = SpecExtractor(client)
    result = extractor.extract(raw_page)

    specs = result.configuration_graph["specs"]
    assert specs["length_mm"] == 131
    assert specs["weight_g"] == 110
    assert "aluminum" in specs["material"].lower()
    assert specs["max_lumens"] == 5200


# --- Behavior 3: High confidence when full spec table present ---

def test_extract_assigns_high_confidence_for_complete_data():
    from src.extractor.spec_extractor import SpecExtractor

    markdown = (FIXTURES / "sofirn_multi_variant_page.md").read_text(encoding="utf-8")
    raw_page = make_raw_page(markdown)
    client = fake_anthropic_client(SC33_GRAPH)

    extractor = SpecExtractor(client)
    result = extractor.extract(raw_page)

    assert result.confidence_tier == "high"
    assert result.confidence_score >= 0.7


# --- Behavior 4: Low confidence when markdown has no structured specs ---

def test_extract_assigns_low_confidence_for_sparse_markdown():
    from src.extractor.spec_extractor import SpecExtractor

    sparse_graph = {
        "product_name": "",
        "brand": "sofirn",
        "leds": [],
        "drivers": [],
        "pairings": [],
        "specs": {"length_mm": None, "weight_g": None, "material": None, "max_lumens": None},
        "price": None,
        "source_url": None,
    }
    raw_page = make_raw_page("Sofirn flashlight. Buy now.")
    client = fake_anthropic_client(sparse_graph)

    extractor = SpecExtractor(client)
    result = extractor.extract(raw_page)

    assert result.confidence_tier == "low"
    assert result.confidence_score < 0.4


# --- Behavior 5: Prompt version is stored on every extraction ---

def test_extract_stores_prompt_version():
    from src.extractor.spec_extractor import SpecExtractor

    raw_page = make_raw_page("Some flashlight markdown.")
    client = fake_anthropic_client(SC33_GRAPH)

    extractor = SpecExtractor(client, prompt_version="v42")
    result = extractor.extract(raw_page)

    assert result.extraction_prompt_version == "v42"


# --- Behavior 6: Markdown code fences stripped before parse ---

def test_markdown_fenced_json_is_parsed_correctly():
    from src.extractor.spec_extractor import SpecExtractor

    fenced = f"```json\n{json.dumps(SC33_GRAPH)}\n```"
    content_block = MagicMock()
    content_block.text = fenced
    message = MagicMock()
    message.content = [content_block]
    client = MagicMock()
    client.messages.create.return_value = message

    extractor = SpecExtractor(client)
    result = extractor.extract(make_raw_page("Some markdown."))

    assert result.brand == "sofirn"
    assert result.model == "Sofirn SC33"


# --- Behavior 7: Unparseable response → raises ExtractionError(reason='json_parse_error') ---

def test_unparseable_response_raises_extraction_error():
    from src.extractor.spec_extractor import SpecExtractor, ExtractionError

    garbage = "Sorry, I cannot extract specs from this page. The content is unclear."
    content_block = MagicMock()
    content_block.text = garbage
    message = MagicMock()
    message.content = [content_block]
    client = MagicMock()
    client.messages.create.return_value = message

    extractor = SpecExtractor(client)

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(make_raw_page("Some markdown."))

    err = exc_info.value
    assert err.reason == "json_parse_error"
    assert garbage[:50] in err.detail


# --- Behavior 8: API call fails → raises ExtractionError(reason='api_error') ---

def test_api_error_raises_extraction_error():
    from src.extractor.spec_extractor import SpecExtractor, ExtractionError

    client = MagicMock()
    client.messages.create.side_effect = Exception("Rate limit exceeded. Retry after 60s.")

    extractor = SpecExtractor(client)

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(make_raw_page("Some markdown."))

    err = exc_info.value
    assert err.reason == "api_error"
    assert "Rate limit exceeded" in err.detail
