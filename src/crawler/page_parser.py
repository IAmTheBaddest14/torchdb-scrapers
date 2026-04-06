"""Parse embedded product JSON from Shoplazza/Shopify-style product pages."""
import re
import json
from typing import Any


def parse_variant_options(html: str) -> list[dict[str, Any]]:
    """Extract the options array from embedded product JSON in page HTML.

    Returns a list of dicts with 'name' and 'values' keys.
    Returns empty list if no options JSON found.
    """
    m = re.search(r'"options"\s*:\s*(\[.*?\])\s*,\s*"variants"', html, re.DOTALL)
    if not m:
        return []
    try:
        raw = json.loads(m.group(1))
    except json.JSONDecodeError:
        return []
    return [{"name": o["name"], "values": o["values"]} for o in raw if "name" in o and "values" in o]


def parse_variants(html: str) -> list[dict[str, Any]]:
    """Extract variants from embedded product JSON, resolving option1/option2/option3 to named keys.

    Returns a list of dicts, each with:
      - 'id': variant ID
      - 'title': variant title string
      - 'price': price string
      - 'available': bool
      - 'options': dict mapping option name → value (e.g. {"tint": "6500K"})
    Returns empty list if no variants found.
    """
    option_map = _build_option_position_map(html)
    raw_variants = _extract_raw_variants(html)

    result = []
    for v in raw_variants:
        named_options: dict[str, str] = {}
        for pos, name in option_map.items():
            key = f"option{pos}"
            val = v.get(key, "")
            if val:
                named_options[name] = val
        result.append({
            "id": v.get("id", ""),
            "title": v.get("title", ""),
            "price": v.get("price", ""),
            "available": v.get("available", False),
            "options": named_options,
        })
    return result


def filter_product_urls(urls: list[str], exclude_patterns: list[str]) -> list[str]:
    """Return only URLs whose product slug does not match any exclude pattern.

    Patterns are applied to the last path segment of the URL (the product slug).
    """
    compiled = [re.compile(p) for p in exclude_patterns]
    result = []
    for url in urls:
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        if not any(p.search(slug) for p in compiled):
            result.append(url)
    return result


def parse_image_urls(html: str) -> list[str]:
    """Extract unique absolute image URLs from page HTML.

    Returns deduplicated list of https:// image URLs (jpg, jpeg, png, webp).
    Skips data: URIs and duplicates.
    """
    urls = re.findall(r'https?://[^\s"\']+\.(?:jpg|jpeg|png|webp)[^\s"\']*', html)
    # Deduplicate while preserving order; prefer https over http
    seen: dict[str, str] = {}
    for url in urls:
        # Normalize to https
        canonical = url.replace("http://", "https://", 1)
        if canonical not in seen:
            seen[canonical] = canonical
    return list(seen.keys())


def _build_option_position_map(html: str) -> dict[int, str]:
    """Build a mapping of position → option name from the options array."""
    m = re.search(r'"options"\s*:\s*(\[.*?\])\s*,\s*"variants"', html, re.DOTALL)
    if not m:
        return {}
    try:
        raw = json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}
    return {o.get("position", i + 1): o["name"] for i, o in enumerate(raw) if "name" in o}


def _extract_raw_variants(html: str) -> list[dict[str, Any]]:
    """Extract raw variant objects from the page using an incremental JSON decoder."""
    marker = '"variants":['
    idx = html.find(marker)
    if idx == -1:
        return []
    pos = idx + len(marker)
    decoder = json.JSONDecoder()
    variants: list[dict[str, Any]] = []
    chunk = html[pos:]
    while chunk.startswith("{"):
        try:
            obj, end = decoder.raw_decode(chunk)
            variants.append(obj)
            chunk = chunk[end:].lstrip(",")
        except json.JSONDecodeError:
            break
    return variants
