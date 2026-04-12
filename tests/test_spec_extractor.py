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


def make_raw_page(markdown: str, url: str = "https://sofirnlight.com/products/sc33",
                  raw_variant_data=None) -> RawPage:
    return RawPage(
        id=1,
        crawl_run_id=1,
        url=url,
        markdown=markdown,
        image_urls=[],
        raw_variant_data=raw_variant_data,
        crawled_at=datetime.now(timezone.utc),
        scraper_version="test",
    )


def fake_anthropic_client(response_graph: dict):
    """Return a minimal fake LLM client that yields a canned ConfigurationGraph JSON."""
    client = MagicMock()
    client.complete.return_value = json.dumps(response_graph)
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
    client = MagicMock()
    client.complete.return_value = fenced

    extractor = SpecExtractor(client)
    result = extractor.extract(make_raw_page("Some markdown."))

    assert result.brand == "sofirn"
    assert result.model == "Sofirn SC33"


# --- Behavior 7: Unparseable response → raises ExtractionError(reason='json_parse_error') ---

def test_unparseable_response_raises_extraction_error():
    from src.extractor.spec_extractor import SpecExtractor, ExtractionError

    garbage = "Sorry, I cannot extract specs from this page. The content is unclear."
    client = MagicMock()
    client.complete.return_value = garbage

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
    client.complete.side_effect = Exception("Rate limit exceeded. Retry after 60s.")

    extractor = SpecExtractor(client)

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(make_raw_page("Some markdown."))

    err = exc_info.value
    assert err.reason == "api_error"
    assert "Rate limit exceeded" in err.detail


VARIANT_DATA = {
    "options": [
        {"name": "CCT", "values": ["5000K", "6500K"]},
    ],
    "variants": [
        {"id": "aaa", "options": {"CCT": "5000K"}, "price": "31.99", "available": True},
        {"id": "bbb", "options": {"CCT": "6500K"}, "price": "31.99", "available": True},
    ],
}

SCRAPER_HINTS = {
    "cct_option_names": ["CCT", "Color Temperature", "Tint"],
    "led_option_names": ["LED", "Emitter"],
}


def capture_llm_message(client) -> str:
    """Return the user message string that was sent to the fake LLM."""
    call_args = client.complete.call_args
    messages = call_args.kwargs.get("messages") or []
    content = messages[0]["content"] if messages else ""
    return content if isinstance(content, str) else ""


# --- Behavior 9: Brand is lowercased on ExtractedProduct regardless of LLM casing ---

def test_brand_is_normalized_to_lowercase():
    from src.extractor.spec_extractor import SpecExtractor

    mixed_case_graph = {**SC33_GRAPH, "brand": "  Sofirn  "}
    client = fake_anthropic_client(mixed_case_graph)
    extractor = SpecExtractor(client)

    result = extractor.extract(make_raw_page("Some markdown."))

    assert result.brand == "sofirn"


VARIANT_DATA = {
    "options": [
        {"name": "CCT", "values": ["5000K", "6500K"]},
    ],
    "variants": [
        {"id": "aaa", "options": {"CCT": "5000K"}, "price": "31.99", "available": True},
        {"id": "bbb", "options": {"CCT": "6500K"}, "price": "31.99", "available": True},
    ],
}

SCRAPER_HINTS = {
    "cct_option_names": ["CCT", "Color Temperature", "Tint"],
    "led_option_names": ["LED", "Emitter"],
}


# --- Behavior 10: Variant option names and values appear in the LLM user message ---

def test_variant_options_included_in_llm_message():
    from src.extractor.spec_extractor import SpecExtractor

    client = fake_anthropic_client(SC33_GRAPH)
    extractor = SpecExtractor(client)
    raw_page = make_raw_page("Some markdown.", raw_variant_data=VARIANT_DATA)

    extractor.extract(raw_page)

    msg = capture_llm_message(client)
    assert "CCT" in msg
    assert "5000K" in msg
    assert "6500K" in msg


# --- Behavior 11: Per-variant prices appear in the LLM user message ---

def test_variant_prices_included_in_llm_message():
    from src.extractor.spec_extractor import SpecExtractor

    client = fake_anthropic_client(SC33_GRAPH)
    extractor = SpecExtractor(client)
    raw_page = make_raw_page("Some markdown.", raw_variant_data=VARIANT_DATA)

    extractor.extract(raw_page)

    msg = capture_llm_message(client)
    assert "31.99" in msg


# --- Behavior 12: Scraper hints appear in the LLM message when provided ---

def test_scraper_hints_included_in_llm_message():
    from src.extractor.spec_extractor import SpecExtractor

    client = fake_anthropic_client(SC33_GRAPH)
    extractor = SpecExtractor(client)
    raw_page = make_raw_page("Some markdown.")

    extractor.extract(raw_page, scraper_hints=SCRAPER_HINTS)

    msg = capture_llm_message(client)
    assert "Color Temperature" in msg
    assert "Emitter" in msg


# --- Behavior 13: Spec pass is always text-only, even when UI diagram URL is present ---

def test_extract_with_ui_diagram_url_sends_text_only():
    from src.extractor.spec_extractor import SpecExtractor
    from src.staging.models import RawPage

    raw_page = RawPage(
        id=1,
        crawl_run_id=1,
        url="https://sofirnlight.com/products/st10",
        markdown="Some markdown.",
        image_urls=[],
        raw_variant_data=None,
        crawled_at=datetime.now(timezone.utc),
        scraper_version="test",
        manual_ui_diagram_url="https://example.com/ui.png",
    )
    client = fake_anthropic_client(SC33_GRAPH)
    extractor = SpecExtractor(client)

    extractor.extract(raw_page)

    call_args = client.complete.call_args
    messages = call_args.kwargs.get("messages") or []
    content = messages[0]["content"]
    assert isinstance(content, str), "Spec pass must send text-only — image should not be included"
