"""
Tests for UIExtractor — verifies behavior through public interface only.
Unit tests use a fake Anthropic client with canned vision responses.
"""
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.staging.models import RawPage

KNOWN_FRAMEWORKS = ["anduril_2", "narsilm", "zebralight"]


def make_raw_page(markdown: str = "", image_urls: list[str] = None, raw_variant_data: dict = None) -> RawPage:
    return RawPage(
        id=1,
        crawl_run_id=1,
        url="https://sofirnlight.com/products/test",
        markdown=markdown,
        image_urls=image_urls or [],
        raw_variant_data=raw_variant_data,
        crawled_at=datetime.now(timezone.utc),
        scraper_version="test",
    )


def fake_vision_client(*responses: dict):
    """Return a fake Anthropic client whose vision calls return canned JSON responses in order."""
    call_count = 0

    def create(**kwargs):
        nonlocal call_count
        resp = responses[min(call_count, len(responses) - 1)]
        call_count += 1
        content_block = MagicMock()
        content_block.text = json.dumps(resp)
        message = MagicMock()
        message.content = [content_block]
        return message

    client = MagicMock()
    client.messages.create.side_effect = create
    return client


# --- Behavior 1: Returns empty list when no images and no UI mentions in markdown ---

def test_extract_returns_empty_list_when_no_ui_content():
    from src.extractor.ui_extractor import UIExtractor

    raw_page = make_raw_page(
        markdown="Buy the SC33 flashlight. 5200 lumens. Great for camping.",
        image_urls=[],
    )
    client = fake_vision_client()
    extractor = UIExtractor(client)

    result = extractor.extract(raw_page)

    assert result == []
    client.messages.create.assert_not_called()


# --- Behavior 2: Detects known framework by name in markdown — no vision call ---

def test_extract_detects_known_framework_from_text():
    from src.extractor.ui_extractor import UIExtractor

    raw_page = make_raw_page(
        markdown="This flashlight runs Anduril 2 firmware. Highly programmable.",
        image_urls=["https://img.fantaskycdn.com/some_product_photo.jpg"],
    )
    client = fake_vision_client()
    extractor = UIExtractor(client)

    result = extractor.extract(raw_page)

    assert len(result) == 1
    assert result[0]["framework"] == "anduril_2"
    assert result[0]["framework_source"] == "known"
    assert result[0]["needs_review"] is False
    assert result[0]["tags"]["programmable"] is True
    assert result[0]["tags"]["complexity"] == "Complex"
    # Should NOT have made any vision calls
    client.messages.create.assert_not_called()


# --- Behavior 3: Returns multiple UIInstances when images map to different UI variants ---

def test_extract_returns_multiple_instances_for_variant_images():
    from src.extractor.ui_extractor import UIExtractor

    anduril_response = {
        "is_ui_diagram": True,
        "framework": "Anduril 2",
        "diagram_label": "Anduril 2 UI",
        "mermaid": "graph TD\n  A[Off] --> B[On]",
        "completeness": 0.9,
        "tags": {
            "complexity": "Complex", "programmable": True, "tactical": True,
            "has_moonlight": True, "has_strobe": True, "has_turbo": True,
            "memory_mode": True, "button_count": 1,
        },
    }
    simple_response = {
        "is_ui_diagram": True,
        "framework": None,
        "diagram_label": "4 Group UI",
        "mermaid": "graph TD\n  A[Off] --> B[Group 1]",
        "completeness": 0.95,
        "tags": {
            "complexity": "Simple", "programmable": False, "tactical": False,
            "has_moonlight": False, "has_strobe": False, "has_turbo": False,
            "memory_mode": False, "button_count": 1,
        },
    }

    raw_page = make_raw_page(
        markdown="Available in Anduril 2 or 4-group firmware.",
        image_urls=[
            "https://example.com/anduril_diagram.jpg",
            "https://example.com/4group_diagram.jpg",
        ],
        raw_variant_data={
            "options": [{"name": "firmware", "values": ["Anduril 2", "4 groups"]}],
            "variants": [],
        },
    )
    client = fake_vision_client(anduril_response, simple_response)
    extractor = UIExtractor(client)

    result = extractor.extract(raw_page)

    assert len(result) == 2
    frameworks = {r["framework"] for r in result}
    assert "anduril_2" in frameworks
    assert "proprietary" in frameworks


# --- Behavior 4: Sets needs_review=True when LLM returns low completeness ---

def test_extract_sets_needs_review_for_low_completeness():
    from src.extractor.ui_extractor import UIExtractor

    partial_response = {
        "is_ui_diagram": True,
        "framework": None,
        "diagram_label": "Partial UI",
        "mermaid": "graph TD\n  A[Off] --> B[On]",
        "completeness": 0.3,
        "tags": {
            "complexity": "Complex", "programmable": False, "tactical": False,
            "has_moonlight": False, "has_strobe": False, "has_turbo": False,
            "memory_mode": False, "button_count": 1,
        },
    }

    raw_page = make_raw_page(
        markdown="Complex UI diagram below.",
        image_urls=["https://example.com/complex_ui.jpg"],
    )
    client = fake_vision_client(partial_response)
    extractor = UIExtractor(client)

    result = extractor.extract(raw_page)

    assert len(result) == 1
    assert result[0]["needs_review"] is True
    assert result[0]["diagrams"][0]["completeness"] == 0.3


# --- Behavior 5: Skips non-UI images, only UI diagrams make it into results ---

def test_extract_skips_non_ui_images():
    from src.extractor.ui_extractor import UIExtractor

    product_photo_response = {"is_ui_diagram": False}
    ui_response = {
        "is_ui_diagram": True,
        "framework": None,
        "diagram_label": "Mode Chart",
        "mermaid": "graph TD\n  A[Off] --> B[Low]",
        "completeness": 0.85,
        "tags": {
            "complexity": "Simple", "programmable": False, "tactical": False,
            "has_moonlight": True, "has_strobe": False, "has_turbo": False,
            "memory_mode": False, "button_count": 1,
        },
    }

    raw_page = make_raw_page(
        markdown="Great flashlight.",
        image_urls=[
            "https://example.com/product_photo_1.jpg",
            "https://example.com/ui_diagram.jpg",
            "https://example.com/product_photo_2.jpg",
        ],
    )
    # First and third images return non-UI, second returns UI
    client = fake_vision_client(product_photo_response, ui_response, product_photo_response)
    extractor = UIExtractor(client)

    result = extractor.extract(raw_page)

    assert len(result) == 1
    assert result[0]["diagrams"][0]["label"] == "Mode Chart"
    # All 3 images were checked
    assert client.messages.create.call_count == 3
