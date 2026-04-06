"""ScraperPipeline — orchestrates the full two-phase crawl → extract → promote pipeline."""
from dataclasses import dataclass, field
from typing import Any

_SCRAPER_VERSION = "0.1.0"


@dataclass
class PipelineResult:
    pages_crawled: int = 0
    products_extracted: int = 0
    promotion_results: list = field(default_factory=list)


class ScraperPipeline:
    def __init__(self, page_crawler, spec_extractor, promotion_engine, repo,
                 scraper_version: str = _SCRAPER_VERSION):
        self._crawler = page_crawler
        self._extractor = spec_extractor
        self._promotion = promotion_engine
        self._repo = repo
        self._version = scraper_version

    async def run(
        self,
        phase: str = "both",
        urls: list[str] | None = None,
        promote: bool = False,
        dry_run: bool = False,
        brand: str = "sofirn",
    ) -> PipelineResult:
        """Run the pipeline for the given phase.

        phase='crawl'   — Phase 1 only: crawl pages → raw_pages
        phase='extract' — Phase 2 only: raw_pages → extracted_products (no re-crawl)
        phase='both'    — Both phases in sequence
        promote=True    — Also run PromotionEngine after extraction
        dry_run=True    — Calculate all decisions, write nothing
        """
        result = PipelineResult()

        if phase in ("crawl", "both"):
            crawl_run = self._repo.create_crawl_run(
                brand=brand, scraper_version=self._version
            )
            pages = await self._run_crawl(crawl_run.id, urls or [], dry_run)
            result.pages_crawled = len(pages)
            if not dry_run:
                self._repo.complete_crawl_run(crawl_run.id, len(pages))

            if phase == "both" and not dry_run:
                extracted = self._run_extract(crawl_run.id, dry_run)
                result.products_extracted = len(extracted)
                if promote:
                    result.promotion_results = self._run_promote(extracted)

        elif phase == "extract":
            latest = self._repo.get_latest_crawl_run(brand=brand)
            if not latest:
                print(f"No crawl runs found for brand '{brand}'. Run --phase crawl first.")
                return result
            raw_pages = self._repo.get_raw_pages_for_run(crawl_run_id=latest.id)
            extracted = self._run_extract_pages(raw_pages, dry_run)
            result.products_extracted = len(extracted)
            if promote:
                result.promotion_results = self._run_promote(extracted)

        return result

    async def _run_crawl(self, crawl_run_id: int, urls: list[str], dry_run: bool) -> list:
        pages = []
        for url in urls:
            raw_page = await self._crawler.crawl_product(
                url=url, crawl_run_id=crawl_run_id, repo=self._repo
            )
            if raw_page is not None:
                pages.append(raw_page)
        return pages

    def _run_extract(self, crawl_run_id: int, dry_run: bool) -> list:
        raw_pages = self._repo.get_raw_pages_for_run(crawl_run_id=crawl_run_id)
        return self._run_extract_pages(raw_pages, dry_run)

    def _run_extract_pages(self, raw_pages: list, dry_run: bool) -> list:
        extracted = []
        for raw_page in raw_pages:
            product = self._extractor.extract(raw_page)
            if product and not dry_run:
                self._repo.save_extracted_product(
                    raw_page_id=raw_page.id,
                    brand=product.brand,
                    model=product.model,
                    configuration_graph=product.configuration_graph,
                    confidence_score=product.confidence_score,
                    confidence_tier=product.confidence_tier,
                    extraction_prompt_version=product.extraction_prompt_version,
                )
            if product:
                extracted.append(product)
        return extracted

    def _run_promote(self, extracted_products: list) -> list:
        results = []
        for product in extracted_products:
            results.extend(self._promotion.promote(product))
        return results
