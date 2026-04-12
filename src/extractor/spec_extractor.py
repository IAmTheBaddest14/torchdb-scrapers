"""SpecExtractor — Phase 2: LLM extraction from stored RawPage markdown."""
import json
import re
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel

from src.staging.models import ExtractedProduct, RawPage


# --- Pydantic schema for structured output (used by Ollama grammar-constrained decoding) ---

class _LED(BaseModel):
    name: str
    cct_hints: list[str] = []

class _Driver(BaseModel):
    name: str

class _Pairing(BaseModel):
    led: str
    driver: str

class _Specs(BaseModel):
    length_mm: float | None = None
    weight_g: float | None = None
    material: str | None = None
    max_lumens: float | None = None

class _Battery(BaseModel):
    type: str
    capacity_mah: float | None = None
    included: bool = False
    removable: bool = True

class _Mode(BaseModel):
    name: str
    output_lm: float | None = None
    runtime_h: float | None = None
    distance_m: float | None = None
    intensity_cd: float | None = None

class _LightModes(BaseModel):
    light: str
    modes: list[_Mode] = []

class FlashlightExtraction(BaseModel):
    product_name: str = ""
    brand: str = ""
    leds: list[_LED] = []
    drivers: list[_Driver] = []
    pairings: list[_Pairing] = []
    specs: _Specs = _Specs()
    batteries: list[_Battery] = []
    tags: list[str] = []
    compatible_accessories: list[str] = []
    mode_data: list[_LightModes] = []
    price: str | None = None

FLASHLIGHT_JSON_SCHEMA = FlashlightExtraction.model_json_schema()


class ExtractionError(Exception):
    """Raised when LLM extraction fails. Carries structured reason and detail."""
    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail

PROMPT_VERSION = "v6"

_SYSTEM_PROMPT = """You are a flashlight specification extractor. Given markdown from a flashlight product page (and optionally a product manual and UI diagram image), extract structured data and return ONLY valid JSON with this exact shape:

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
  "batteries": [
    {"type": "14500", "capacity_mah": null, "included": false, "removable": true}
  ],
  "tags": ["usb-charging", "magnetic-tail", "clip", "side-switch", "tail-switch", "aux-light"],
  "compatible_accessories": ["21700 extension tube"],
  "mode_data": [
    {
      "light": "Main Light",
      "modes": [
        {"name": "Turbo", "output_lm": 1000, "runtime_h": 1.33, "distance_m": 144, "intensity_cd": 5175}
      ]
    }
  ],
  "price": "<string or null>"
}

Battery type must be the cell format string (e.g. "14500", "21700", "18650", "AA", "AAA", "built-in").
Set removable=false only for built-in batteries. Set included=true only if the battery ships in the box.
If the same LED name appears in multiple CCT variants, list it once in leds with all CCT hints combined. Use the shared LED name in pairings.
Tags must be lowercase hyphenated slugs. Only include tags that are explicitly supported by the product.
Common tags: usb-charging, magnetic-tail, clip, side-switch, tail-switch, rear-switch, aux-light, dual-switch, lockout-mode, strobe, waterproof.
Battery indicator tags: always emit "battery-indicator" if any indicator is present, plus a subtype tag when clearly stated — "battery-indicator-led" (colour-coded LED), "battery-indicator-voltage" (numeric voltage display), "battery-indicator-percentage" (percentage display), "battery-indicator-blink" (blink pattern).
compatible_accessories: list accessories mentioned as compatible (e.g. extension tubes, diffusers). Empty array if none.
mode_data: extract ALL light sources and ALL modes from the PDF spec table (ANSI/NEMA FL1 chart). Use null for missing values. Empty array if no mode data available. Do NOT use the UI diagram image for quantitative values — it shows button sequences only.

Return only the JSON object. No markdown fences, no explanation."""


def _build_variant_context(raw_variant_data: dict | None, scraper_hints: dict | None) -> str:
    """Format variant data and scraper hints into a structured prompt section."""
    parts: list[str] = []

    if raw_variant_data:
        options = raw_variant_data.get("options", [])
        if options:
            parts.append("## Variant Options")
            for opt in options:
                values = ", ".join(str(v) for v in opt.get("values", []))
                parts.append(f"- {opt['name']}: {values}")

        variants = raw_variant_data.get("variants", [])
        if variants:
            parts.append("\n## Variant Details")
            # Build header from option names
            first = variants[0]
            opt_keys = list(first.get("options", {}).keys())
            header = " | ".join(opt_keys + ["Price", "Available"])
            separator = " | ".join(["---"] * (len(opt_keys) + 2))
            parts.append(f"| {header} |")
            parts.append(f"| {separator} |")
            for v in variants:
                opt_vals = [str(v["options"].get(k, "")) for k in opt_keys]
                price = v.get("price", "")
                available = "Yes" if v.get("available") else "No"
                row = " | ".join(opt_vals + [price, available])
                parts.append(f"| {row} |")

    if scraper_hints:
        parts.append("\n## Scraper Hints")
        for key, values in scraper_hints.items():
            label = key.replace("_", " ")
            parts.append(f"- {label}: {', '.join(str(v) for v in values)}")

    return "\n".join(parts)


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
    def __init__(self, llm_client, prompt_version: str = PROMPT_VERSION):
        self._client = llm_client
        self._prompt_version = prompt_version

    def extract(self, raw_page: RawPage, scraper_hints: dict | None = None) -> ExtractedProduct:
        """Send raw_page.markdown (+ variant data + hints) to Claude and return an ExtractedProduct.

        Does not save to the database — caller is responsible for persisting via repo.
        """
        markdown = raw_page.markdown or ""
        variant_context = _build_variant_context(raw_page.raw_variant_data, scraper_hints)
        text_content = f"Extract specs from this flashlight page:\n\n{markdown}"
        if variant_context:
            text_content += f"\n\n{variant_context}"
        if raw_page.manual_pdf_text:
            truncated = raw_page.manual_pdf_text[:4000]
            text_content += f"\n\n## Product Manual (PDF)\n{truncated}"

        user_content = text_content

        try:
            raw_json = self._client.complete(
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_content}],
                max_tokens=4096,
                response_schema=FLASHLIGHT_JSON_SCHEMA,
            )
        except Exception as e:
            raise ExtractionError(reason="api_error", detail=str(e))
        # Strip markdown code fences if Claude wrapped the response
        raw_json = re.sub(r"^```(?:json)?\s*", "", raw_json)
        raw_json = re.sub(r"\s*```$", "", raw_json).strip()

        try:
            graph: dict[str, Any] = json.loads(raw_json)
        except json.JSONDecodeError:
            raise ExtractionError(
                reason="json_parse_error",
                detail=raw_json[:200],
            )

        # Always use the actual crawled URL — LLM guesses are unreliable
        graph["source_url"] = raw_page.url

        # Set hosted PDF URL (overrides anything the LLM may have guessed)
        graph["manual_pdf_url"] = raw_page.manual_pdf_url

        # Ensure new array fields always exist
        graph.setdefault("batteries", [])
        graph.setdefault("tags", [])
        graph.setdefault("compatible_accessories", [])
        graph.setdefault("mode_data", [])

        # Set UI diagram URL from crawler (authoritative)
        graph["ui_diagram_url"] = raw_page.manual_ui_diagram_url

        score, tier = _confidence(graph)

        return ExtractedProduct(
            id=0,
            raw_page_id=raw_page.id,
            brand=(graph.get("brand") or "").strip().lower(),
            model=(graph.get("product_name") or "").strip(),
            configuration_graph=graph,
            confidence_score=score,
            confidence_tier=tier,
            extraction_prompt_version=self._prompt_version,
            extracted_at=datetime.now(timezone.utc),
        )
