"""
Tests for PageCrawler — verifies behavior through public interface only.
Offline tests use real fixture HTML from Sofirn product pages.
Integration tests (marked with @pytest.mark.integration) hit the live site.
"""
import pytest
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


# --- Behavior 1: Extract variant options from embedded product JSON ---

def test_extract_variant_options_from_fixture():
    from src.crawler.page_parser import parse_variant_options

    html = (FIXTURES / "sofirn_multi_variant_page.html").read_text(encoding="utf-8")
    options = parse_variant_options(html)

    # SC33 fixture has: style + tint options
    option_names = [o["name"] for o in options]
    assert "tint" in option_names

    tint = next(o for o in options if o["name"] == "tint")
    assert "6500K" in tint["values"]
    assert "5000K" in tint["values"]


# --- Behavior 2: Variants resolve option1/option2 keys to option names ---

def test_parse_variants_maps_options_to_names():
    from src.crawler.page_parser import parse_variants

    html = (FIXTURES / "sofirn_multi_variant_page.html").read_text(encoding="utf-8")
    variants = parse_variants(html)

    assert len(variants) > 0
    first = variants[0]
    # Each variant should have 'options' dict keyed by option name, not option1/option2
    assert "options" in first
    assert "tint" in first["options"]
    assert first["options"]["tint"] in ("6500K", "5000K")


# --- Behavior 3: Image URLs extracted from fixture ---

def test_extract_image_urls_from_fixture():
    from src.crawler.page_parser import parse_image_urls

    html = (FIXTURES / "sofirn_multi_variant_page.html").read_text(encoding="utf-8")
    image_urls = parse_image_urls(html)

    assert len(image_urls) > 0
    # All returned URLs should be absolute HTTP(S) URLs
    for url in image_urls:
        assert url.startswith("http"), f"Expected absolute URL, got: {url}"
    # SC33 fixture images are on fantaskycdn.com or staticdj.com
    domains = {url.split("/")[2] for url in image_urls}
    assert any("cdn" in d or "staticdj" in d for d in domains), f"Expected CDN domain, got: {domains}"


# --- Behavior 4: Exclude patterns filter product URLs ---

def test_exclude_patterns_filter_product_urls():
    from src.crawler.page_parser import filter_product_urls

    urls = [
        "https://sofirnlight.com/products/sc33-flashlight",
        "https://sofirnlight.com/products/18650-battery-pack",
        "https://sofirnlight.com/products/sc33-charger-kit",
        "https://sofirnlight.com/products/replacement-lens",
        "https://sofirnlight.com/products/sp36-pro",
    ]
    exclude_patterns = ["(?i)battery", "(?i)charger", "(?i)^replacement"]

    result = filter_product_urls(urls, exclude_patterns)

    assert "https://sofirnlight.com/products/sc33-flashlight" in result
    assert "https://sofirnlight.com/products/sp36-pro" in result
    assert "https://sofirnlight.com/products/18650-battery-pack" not in result
    assert "https://sofirnlight.com/products/sc33-charger-kit" not in result
    # 'replacement-lens' path segment doesn't start with replacement but contains it
    # The pattern matches against the URL path segment (product slug)
    assert "https://sofirnlight.com/products/replacement-lens" not in result


# --- Behavior 5: Integration — crawl real URL and save raw_page to Supabase ---

@pytest.mark.integration
@pytest.mark.asyncio
async def test_crawl_product_url_saves_raw_page(repo):
    from src.crawler.page_crawler import PageCrawler
    from src.config.brand_config import BrandConfig

    config = BrandConfig.load("sofirn")
    crawler = PageCrawler(config)

    url = "https://sofirnlight.com/products/sofirn-sc33-edc-flashlight-5200lm"
    run = repo.create_crawl_run(brand="sofirn", scraper_version="test")

    raw_page = await crawler.crawl_product(url, crawl_run_id=run.id, repo=repo)

    assert raw_page is not None
    assert raw_page.url == url
    assert raw_page.markdown and len(raw_page.markdown) > 500
    assert len(raw_page.image_urls) > 0
    assert raw_page.raw_variant_data is not None
    assert "options" in raw_page.raw_variant_data
    assert "variants" in raw_page.raw_variant_data
