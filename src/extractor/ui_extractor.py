"""UIExtractor — detects UI frameworks and extracts UI diagrams from product pages."""
import json
import re
from typing import Any

from src.staging.models import RawPage

# Known framework detection: maps search terms to canonical framework slug
KNOWN_FRAMEWORK_PATTERNS: dict[str, str] = {
    r"anduril\s*2?": "anduril_2",
    r"narsil\s*m?": "narsilm",
    r"zebralight\s*ui": "zebralight",
}

_VISION_SYSTEM_PROMPT = """You are a flashlight UI diagram analyzer. Given an image, determine if it shows a UI flowchart or mode diagram for a flashlight.

Return ONLY valid JSON with this exact shape:
{
  "is_ui_diagram": true,
  "framework": "framework name if recognized, or null",
  "diagram_label": "short label for this diagram",
  "mermaid": "graph TD\\n  ...",
  "completeness": 0.85,
  "tags": {
    "complexity": "Simple|Moderate|Complex",
    "programmable": false,
    "tactical": false,
    "has_moonlight": false,
    "has_strobe": false,
    "has_turbo": false,
    "memory_mode": false,
    "button_count": 1
  }
}

If the image is NOT a UI diagram (product photo, spec chart, packaging), return:
{"is_ui_diagram": false}

completeness is 0.0-1.0: how fully the mermaid captures what you see. Be honest."""


def _detect_known_framework_in_text(markdown: str) -> str | None:
    """Scan markdown for mentions of known UI frameworks. Returns slug or None."""
    for pattern, slug in KNOWN_FRAMEWORK_PATTERNS.items():
        if re.search(pattern, markdown, re.IGNORECASE):
            return slug
    return None


def _default_tags_for_known_framework(slug: str) -> dict[str, Any]:
    known: dict[str, dict[str, Any]] = {
        "anduril_2": {
            "complexity": "Complex", "programmable": True, "tactical": True,
            "has_moonlight": True, "has_strobe": True, "has_turbo": True,
            "memory_mode": True, "button_count": 1,
        },
        "narsilm": {
            "complexity": "Complex", "programmable": True, "tactical": False,
            "has_moonlight": True, "has_strobe": True, "has_turbo": True,
            "memory_mode": True, "button_count": 1,
        },
        "zebralight": {
            "complexity": "Moderate", "programmable": False, "tactical": False,
            "has_moonlight": True, "has_strobe": True, "has_turbo": True,
            "memory_mode": True, "button_count": 1,
        },
    }
    return known.get(slug, {
        "complexity": "Moderate", "programmable": False, "tactical": False,
        "has_moonlight": False, "has_strobe": False, "has_turbo": False,
        "memory_mode": False, "button_count": 1,
    })


class UIExtractor:
    def __init__(self, anthropic_client):
        self._client = anthropic_client

    def extract(self, raw_page: RawPage) -> list[dict[str, Any]]:
        """Extract UI instances from a raw page.

        Three paths (in order):
        1. Multi-variant — raw_variant_data has multiple UI option values → vision path
        2. Text scan — single known framework in markdown, no variant ambiguity (no API calls)
        3. Image vision — scan candidate images
        4. No UI found → return []
        """
        markdown = raw_page.markdown or ""
        image_urls = raw_page.image_urls or []

        # Path 1: multiple UI variants in raw_variant_data → must use images
        if image_urls and self._has_multiple_ui_variants(raw_page):
            return self._extract_from_images(image_urls, raw_page)

        # Path 2: single known framework detectable from text alone
        framework_slug = _detect_known_framework_in_text(markdown)
        if framework_slug:
            variant_hint = self._extract_variant_hint(raw_page, framework_slug)
            return [self._known_framework_instance(framework_slug, variant_hint)]

        # Path 3: vision scan of images
        if not image_urls:
            return []

        return self._extract_from_images(image_urls, raw_page)

    def _extract_from_images(self, image_urls: list[str], raw_page: RawPage) -> list[dict[str, Any]]:
        """Send each image to Claude vision, collect UI diagrams found."""
        variant_hints = self._build_variant_hint_map(raw_page)
        instances: list[dict[str, Any]] = []

        for url in image_urls:
            response = self._call_vision(url)
            if not response.get("is_ui_diagram"):
                continue

            framework_raw = response.get("framework")
            framework_slug = self._normalize_framework(framework_raw)
            completeness = float(response.get("completeness", 0.5))

            # If vision recognizes a known framework, use pre-built tags
            if framework_slug in KNOWN_FRAMEWORK_PATTERNS.values():
                tags = _default_tags_for_known_framework(framework_slug)
                framework_source = "known"
            else:
                tags = response.get("tags", {})
                framework_source = "extracted"
                framework_slug = framework_slug or "proprietary"

            variant_hint = variant_hints.get(framework_raw or "", None)

            instances.append({
                "variant_hint": variant_hint,
                "framework": framework_slug,
                "framework_source": framework_source,
                "switchable": False,
                "diagrams": [{
                    "label": response.get("diagram_label", "UI Diagram"),
                    "mermaid": response.get("mermaid", ""),
                    "completeness": completeness,
                }],
                "needs_review": completeness < 0.7,
                "tags": tags,
            })

        return instances

    def _call_vision(self, image_url: str) -> dict[str, Any]:
        message = self._client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=_VISION_SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "url", "url": image_url}},
                    {"type": "text", "text": "Analyze this image."},
                ],
            }],
        )
        raw = message.content[0].text.strip()
        return json.loads(raw)

    def _known_framework_instance(self, slug: str, variant_hint: str | None) -> dict[str, Any]:
        return {
            "variant_hint": variant_hint,
            "framework": slug,
            "framework_source": "known",
            "switchable": False,
            "diagrams": [],
            "needs_review": False,
            "tags": _default_tags_for_known_framework(slug),
        }

    def _has_multiple_ui_variants(self, raw_page: RawPage) -> bool:
        """Return True if raw_variant_data contains a UI/firmware option with multiple values."""
        if not raw_page.raw_variant_data:
            return False
        ui_keywords = {"ui", "firmware", "driver", "mode"}
        for option in raw_page.raw_variant_data.get("options", []):
            name = option.get("name", "").lower()
            if any(k in name for k in ui_keywords):
                return len(option.get("values", [])) > 1
            # Also treat any option whose values mention known frameworks
            values = option.get("values", [])
            matches = sum(
                1 for v in values
                if any(re.search(p, v, re.IGNORECASE) for p in KNOWN_FRAMEWORK_PATTERNS)
            )
            if matches > 0:
                return len(values) > 1
        return False

    def _extract_variant_hint(self, raw_page: RawPage, framework_slug: str) -> str | None:
        """Find the variant option value that corresponds to this framework."""
        if not raw_page.raw_variant_data:
            return None
        for option in raw_page.raw_variant_data.get("options", []):
            for value in option.get("values", []):
                if re.search(framework_slug.replace("_", r"\s*"), value, re.IGNORECASE):
                    return value
        return None

    def _build_variant_hint_map(self, raw_page: RawPage) -> dict[str, str]:
        """Build a map of framework name → variant option value from raw_variant_data."""
        if not raw_page.raw_variant_data:
            return {}
        hints: dict[str, str] = {}
        for option in raw_page.raw_variant_data.get("options", []):
            for value in option.get("values", []):
                for pattern, slug in KNOWN_FRAMEWORK_PATTERNS.items():
                    if re.search(pattern, value, re.IGNORECASE):
                        hints[slug] = value
                hints[value] = value
        return hints

    @staticmethod
    def _normalize_framework(raw: str | None) -> str | None:
        if not raw:
            return None
        normalized = raw.lower().replace(" ", "_")
        # Map common variations
        if "anduril" in normalized:
            return "anduril_2"
        if "narsil" in normalized:
            return "narsilm"
        if "zebralight" in normalized:
            return "zebralight"
        return normalized
