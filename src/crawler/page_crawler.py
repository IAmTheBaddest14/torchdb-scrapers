"""PageCrawler — fetches product pages via Crawl4AI and saves to staging."""
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from src.config.brand_config import BrandConfig
from src.crawler.page_parser import parse_variant_options, parse_variants, parse_image_urls
from src.staging.models import RawPage
from src.staging.repository import StagingRepository

_SCRAPER_VERSION = "0.1.0"


class PageCrawler:
    def __init__(self, config: BrandConfig, scraper_version: str = _SCRAPER_VERSION):
        self.config = config
        self.scraper_version = scraper_version

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

        return repo.save_raw_page(
            crawl_run_id=crawl_run_id,
            url=url,
            markdown=result.markdown,
            image_urls=image_urls,
            raw_variant_data=raw_variant_data or None,
            scraper_version=self.scraper_version,
        )
