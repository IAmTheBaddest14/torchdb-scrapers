"""PageCrawler — fetches product pages via Crawl4AI and saves to staging."""
import re
from urllib.parse import urlparse, urljoin

import fitz  # PyMuPDF
import httpx
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from src.config.brand_config import BrandConfig
from src.crawler.page_parser import parse_variant_options, parse_variants, parse_image_urls, parse_product_urls, filter_product_urls
from src.staging.models import RawPage
from src.staging.repository import StagingRepository

_SCRAPER_VERSION = "0.1.0"
_PDF_LINK_RE = re.compile(r'(?:href|src)=["\']([^"\']*\.pdf(?:\?[^"\']*)?)["\']', re.IGNORECASE)
_MANUAL_URL_RE = re.compile(r"manual|guide|instruction", re.IGNORECASE)


class PageCrawler:
    def __init__(self, config: BrandConfig, scraper_version: str = _SCRAPER_VERSION):
        self.config = config
        self.scraper_version = scraper_version

    async def discover_urls(self) -> list[str]:
        """Crawl all collection pages and return deduplicated, filtered product URLs."""
        browser_config = BrowserConfig(headless=True, viewport_width=1920, viewport_height=1080)
        run_config = CrawlerRunConfig(page_timeout=30000, remove_overlay_elements=True)

        all_urls: dict[str, None] = {}
        for path in self.config.collection_paths:
            url = self.config.base_url.rstrip("/") + path
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url, config=run_config)
            if not result.success or not result.html:
                continue
            page_urls = parse_product_urls(result.html, self.config.base_url)
            for u in page_urls:
                all_urls[u] = None

        filtered = filter_product_urls(list(all_urls), self.config.exclude_patterns)
        return filtered

    async def crawl_product(
        self,
        url: str,
        crawl_run_id: int,
        repo: StagingRepository,
    ) -> RawPage | None:
        """Crawl a product page and save the result as a RawPage in staging.

        Returns the saved RawPage, or None if the crawl failed.
        """
        browser_config = BrowserConfig(headless=True, viewport_width=1920, viewport_height=1080)
        run_config = CrawlerRunConfig(page_timeout=30000, remove_overlay_elements=True)

        async with AsyncWebCrawler(config=browser_config) as crawler:
            result = await crawler.arun(url, config=run_config)

        if not result.success or len(result.html or "") < 5000:
            return None

        html = result.html
        image_urls = parse_image_urls(html)
        options = parse_variant_options(html)
        variants = parse_variants(html)

        raw_variant_data: dict = {}
        if options or variants:
            raw_variant_data = {"options": options, "variants": variants}

        manual_pdf_url, manual_pdf_text, manual_ui_diagram_url = await self._fetch_pdf(html, url, repo)

        return repo.save_raw_page(
            crawl_run_id=crawl_run_id,
            url=url,
            markdown=result.markdown,
            image_urls=image_urls,
            raw_variant_data=raw_variant_data or None,
            scraper_version=self.scraper_version,
            manual_pdf_url=manual_pdf_url,
            manual_pdf_text=manual_pdf_text,
            manual_ui_diagram_url=manual_ui_diagram_url,
        )

    async def _fetch_pdf(
        self, html: str, page_url: str, repo: StagingRepository
    ) -> tuple[str | None, str | None, str | None]:
        """Find, download, and upload the product manual PDF.

        Returns (hosted_pdf_url, pdf_text, ui_diagram_url).
        """
        pdf_url = self._find_pdf_url(html, page_url)
        if not pdf_url:
            return None, None, None

        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
                response = await client.get(pdf_url)
                response.raise_for_status()
                pdf_bytes = response.content
        except Exception as e:
            print(f"  [PDF] Download failed for {pdf_url}: {e}")
            return None, None, None

        slug = urlparse(page_url).path.rstrip("/").split("/")[-1]

        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            pdf_text = "\n".join(
                page.get_text() for page in doc
            ).strip()
        except Exception as e:
            print(f"  [PDF] Text extraction failed: {e}")
            pdf_text = None
            doc = None

        try:
            hosted_url = repo.upload_pdf(self.config.brand, slug, pdf_bytes)
            print(f"  [PDF] Uploaded → {hosted_url}")
        except Exception as e:
            print(f"  [PDF] Upload failed: {e}")
            hosted_url = pdf_url  # fall back to manufacturer URL

        return hosted_url, pdf_text, None

    @staticmethod
    def _find_pdf_url(html: str, page_url: str) -> str | None:
        """Return the best PDF URL from the page HTML.

        Prefers URLs whose path contains 'manual', 'guide', or 'instruction'.
        Falls back to the first PDF link found.
        """
        candidates = []
        for match in _PDF_LINK_RE.finditer(html):
            raw = match.group(1)
            if raw.startswith("data:"):
                continue
            url = urljoin(page_url, raw)
            if url.startswith("//"):
                url = "https:" + url
            candidates.append(url)

        if not candidates:
            return None

        candidates.sort(
            key=lambda u: bool(_MANUAL_URL_RE.search(u)),
            reverse=True,
        )
        return candidates[0]
