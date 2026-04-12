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


# --- Behavior 5: parse_product_urls extracts absolute product URLs from collection HTML ---

def test_parse_product_urls_returns_absolute_urls_from_collection_page():
    from src.crawler.page_parser import parse_product_urls

    html = (FIXTURES / "sofirn_collection_page.html").read_text(encoding="utf-8")
    urls = parse_product_urls(html, base_url="https://sofirnlight.com")

    assert "https://sofirnlight.com/products/sc33-edc-flashlight" in urls
    assert "https://sofirnlight.com/products/sp36-pro-flashlight" in urls
    assert "https://sofirnlight.com/products/sc21-pro-mini-flashlight" in urls


def test_parse_product_urls_deduplicates():
    from src.crawler.page_parser import parse_product_urls

    html = (FIXTURES / "sofirn_collection_page.html").read_text(encoding="utf-8")
    urls = parse_product_urls(html, base_url="https://sofirnlight.com")

    assert urls.count("https://sofirnlight.com/products/sc33-edc-flashlight") == 1


def test_parse_product_urls_ignores_non_product_links():
    from src.crawler.page_parser import parse_product_urls

    html = (FIXTURES / "sofirn_collection_page.html").read_text(encoding="utf-8")
    urls = parse_product_urls(html, base_url="https://sofirnlight.com")

    assert not any("amazon.com" in u for u in urls)
    assert not any("/collections/" in u for u in urls)


# --- Behavior 6: Multilang boundary detection ---

def _make_mock_page(blocks):
    """Return a mock fitz.Page whose get_text('blocks') yields the given blocks.

    Each block is (x0, y0, x1, y1, text).  Block number and type are appended
    to match the real PyMuPDF tuple format.
    """
    from unittest.mock import MagicMock
    page = MagicMock()
    page.get_text.return_value = [
        (x0, y0, x1, y1, text, 0, 0) for x0, y0, x1, y1, text in blocks
    ]
    return page


def test_multilang_boundary_finds_standalone_language_header():
    """Detects the first non-English section header and returns its y-coordinate."""
    from src.extractor.pdf_diagram_extractor import PDFDiagramExtractor
    page = _make_mock_page([
        (100, 50,  400, 60,  "General Operation\n"),
        (1418, 370, 1468, 382, "(DE)Deutsch\n"),        # standalone — 12 chars
        (1418, 715, 1468, 727, "(FR)Français\n"),       # later — should not be min
    ])
    assert PDFDiagramExtractor._find_multilang_boundary_y(page) == 370


def test_multilang_boundary_skips_toc_entries():
    """TOC lines (long strings with fill characters) must not trigger the boundary."""
    from src.extractor.pdf_diagram_extractor import PDFDiagramExtractor
    long_toc = "(DE)Deutsch " + "·" * 65   # well over 60 chars
    page = _make_mock_page([
        (1416, 85, 1591, 95, long_toc),
    ])
    assert PDFDiagramExtractor._find_multilang_boundary_y(page) is None


def test_multilang_boundary_returns_none_when_absent():
    """Pages with no language markers return None — no crop capping applied."""
    from src.extractor.pdf_diagram_extractor import PDFDiagramExtractor
    page = _make_mock_page([
        (100, 50,  400, 60,  "General Operation\n"),
        (100, 200, 400, 210, "Button functions\n"),
    ])
    assert PDFDiagramExtractor._find_multilang_boundary_y(page) is None


# --- Behavior 6: Content-column x-boundary detection ---

def test_content_column_x_finds_contents_block():
    """Returns the x0 of the CONTENTS heading so the right column is excluded."""
    from src.extractor.pdf_diagram_extractor import PDFDiagramExtractor
    page = _make_mock_page([
        (820,  25, 869,  50, "Figure 3\n"),
        (1415, 25, 1483, 42, "CONTENTS\n"),
    ])
    assert PDFDiagramExtractor._find_content_column_x(page) == 1415


def test_content_column_x_returns_none_when_absent():
    """Single-column pages (e.g. portrait multi-page manuals) return None — full width used."""
    from src.extractor.pdf_diagram_extractor import PDFDiagramExtractor
    page = _make_mock_page([
        (100, 25, 400, 50, "General Operation\n"),
        (100, 80, 400, 90, "Press button to turn on.\n"),
    ])
    assert PDFDiagramExtractor._find_content_column_x(page) is None


# --- Behavior 7: discover_urls crawls collection paths and returns filtered, deduplicated product URLs ---

@pytest.mark.asyncio
async def test_discover_urls_returns_product_urls_from_collection_pages():
    from unittest.mock import AsyncMock, MagicMock, patch
    from src.crawler.page_crawler import PageCrawler
    from src.config.brand_config import BrandConfig

    collection_html = (FIXTURES / "sofirn_collection_page.html").read_text(encoding="utf-8")

    config = MagicMock(spec=BrandConfig)
    config.base_url = "https://sofirnlight.com"
    config.collection_paths = ["/collections/flashlights"]
    config.exclude_patterns = ["(?i)battery"]

    crawler = PageCrawler(config)

    mock_result = MagicMock()
    mock_result.success = True
    mock_result.html = collection_html

    with patch("src.crawler.page_crawler.AsyncWebCrawler") as mock_crawl4ai:
        mock_instance = AsyncMock()
        mock_instance.arun = AsyncMock(return_value=mock_result)
        mock_crawl4ai.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_crawl4ai.return_value.__aexit__ = AsyncMock(return_value=False)

        urls = await crawler.discover_urls()

    assert "https://sofirnlight.com/products/sc33-edc-flashlight" in urls
    assert "https://sofirnlight.com/products/sp36-pro-flashlight" in urls
    assert not any("battery" in u.lower() for u in urls)


@pytest.mark.asyncio
async def test_discover_urls_deduplicates_across_collection_paths():
    from unittest.mock import AsyncMock, MagicMock, patch
    from src.crawler.page_crawler import PageCrawler
    from src.config.brand_config import BrandConfig

    collection_html = (FIXTURES / "sofirn_collection_page.html").read_text(encoding="utf-8")

    config = MagicMock(spec=BrandConfig)
    config.base_url = "https://sofirnlight.com"
    config.collection_paths = ["/collections/flashlights", "/collections/headlamps"]
    config.exclude_patterns = []

    crawler = PageCrawler(config)

    mock_result = MagicMock()
    mock_result.success = True
    mock_result.html = collection_html

    with patch("src.crawler.page_crawler.AsyncWebCrawler") as mock_crawl4ai:
        mock_instance = AsyncMock()
        mock_instance.arun = AsyncMock(return_value=mock_result)
        mock_crawl4ai.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_crawl4ai.return_value.__aexit__ = AsyncMock(return_value=False)

        urls = await crawler.discover_urls()

    assert urls.count("https://sofirnlight.com/products/sc33-edc-flashlight") == 1


# --- Behavior 8: PageCrawler._fetch_pdf does NOT produce manual_ui_diagram_url ---

@pytest.mark.asyncio
async def test_fetch_pdf_does_not_extract_ui_diagram():
    """PDF download stores url + text on RawPage but never renders a diagram PNG."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from src.crawler.page_crawler import PageCrawler
    from src.config.brand_config import BrandConfig

    config = MagicMock(spec=BrandConfig)
    config.brand = "sofirn"
    config.ui_diagram = {}
    crawler = PageCrawler(config)

    minimal_pdf_html = '<a href="/manual.pdf">manual</a>'
    page_url = "https://sofirnlight.com/products/sc33"

    fake_repo = MagicMock()
    fake_repo.upload_pdf.return_value = "https://storage.example.com/manuals/sc33.pdf"

    fake_pdf_bytes = b"%PDF-1.4 fake"
    mock_doc = MagicMock()
    mock_doc.__iter__ = MagicMock(return_value=iter([]))

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_response = MagicMock()
        mock_response.content = fake_pdf_bytes
        mock_response.raise_for_status = MagicMock()
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        with patch("fitz.open", return_value=mock_doc):
            hosted_url, pdf_text, ui_diagram_url = await crawler._fetch_pdf(
                minimal_pdf_html, page_url, fake_repo
            )

    assert ui_diagram_url is None


# --- Behavior 7: Integration — crawl real URL and save raw_page to Supabase ---

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
