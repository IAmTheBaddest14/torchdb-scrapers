"""SpecExtractor — Phase 2: LLM extraction from stored RawPage markdown."""
import json
from datetime import datetime, timezone
from typing import Any

from src.staging.models import ExtractedProduct, RawPage

PROMPT_VERSION = "v1"

_SYSTEM_PROMPT = """You are a flashlight specification extractor. Given markdown from a flashlight product page, extract structured data and return ONLY valid JSON with this exact shape:

{
  "product_name": "Brand Model",
  "brand": "brand slug",
  "leds": [{"name": "LED name", "cct_hints": ["6500K", "5000K"]}],
  "drivers": [{"name": "driver name"}],
  "pairings": [{"led": "LED name", "driver": "driver name"}],
  "specs": {
    "length_mm": <number or null>,
    "weight_g": <number or null>,
    "material": "<string or null>",
    "max_lumens": <number or null>
  },
  "price": "<string or null>",
  "source_url": "<string or null>"
}

Return only the JSON object. No markdown fences, no explanation."""


def _confidence(graph: dict[str, Any]) -> tuple[float, str]:
    """Score the extraction based on how many fields were populated."""
    specs = graph.get("specs", {})
    spec_fields = [specs.get("length_mm"), specs.get("weight_g"), specs.get("material"), specs.get("max_lumens")]
    filled_specs = sum(1 for v in spec_fields if v is not None)

    has_leds = len(graph.get("leds", [])) > 0
    has_drivers = len(graph.get("drivers", [])) > 0
    has_name = bool(graph.get("product_name"))

    score = (
        (0.3 if has_name else 0.0)
        + (0.3 if has_leds else 0.0)
        + (0.2 if has_drivers else 0.0)
        + (0.2 * filled_specs / max(len(spec_fields), 1))
    )

    if score >= 0.7:
        tier = "high"
    elif score >= 0.4:
        tier = "medium"
    else:
        tier = "low"

    return round(score, 3), tier


class SpecExtractor:
    def __init__(self, anthropic_client, prompt_version: str = PROMPT_VERSION):
        self._client = anthropic_client
        self._prompt_version = prompt_version

    def extract(self, raw_page: RawPage) -> ExtractedProduct:
        """Send raw_page.markdown to Claude and return an ExtractedProduct.

        Does not save to the database — caller is responsible for persisting via repo.
        """
        markdown = raw_page.markdown or ""

        message = self._client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": f"Extract specs from this flashlight page:\n\n{markdown}"}],
        )

        raw_json = message.content[0].text.strip()
        graph: dict[str, Any] = json.loads(raw_json)

        # Ensure source_url falls back to the page URL if LLM left it null
        if not graph.get("source_url"):
            graph["source_url"] = raw_page.url

        score, tier = _confidence(graph)

        return ExtractedProduct(
            id=0,
            raw_page_id=raw_page.id,
            brand=graph.get("brand", ""),
            model=graph.get("product_name", ""),
            configuration_graph=graph,
            confidence_score=score,
            confidence_tier=tier,
            extraction_prompt_version=self._prompt_version,
            extracted_at=datetime.now(timezone.utc),
        )
