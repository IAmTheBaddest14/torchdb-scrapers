"""
Tests for ScraperPipeline — verifies orchestration behavior through the public interface.
All tests use fake collaborators — no Supabase, no live crawls, no LLM calls.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from src.staging.models import RawPage, ExtractedProduct, CrawlRun


# --- Fake collaborators ---

def make_crawl_run(id: int = 1) -> CrawlRun:
    return CrawlRun(
        id=id, brand="sofirn", started_at=datetime.now(timezone.utc),
        completed_at=None, pages_crawled=None, scraper_version="test",
    )


def make_raw_page(id: int = 1, crawl_run_id: int = 1) -> RawPage:
    return RawPage(
        id=id, crawl_run_id=crawl_run_id,
        url="https://sofirnlight.com/products/sc33",
        markdown="SC33 flashlight specs...",
        image_urls=[], raw_variant_data=None,
        crawled_at=datetime.now(timezone.utc), scraper_version="test",
    )


def make_extracted_product(id: int = 1, raw_page_id: int = 1) -> ExtractedProduct:
    return ExtractedProduct(
        id=id, raw_page_id=raw_page_id, brand="sofirn", model="SC33",
        configuration_graph={
            "product_name": "Sofirn SC33", "brand": "sofirn",
            "leds": [{"name": "XHP70.3 HI", "cct_hints": []}],
            "drivers": [{"name": "Boost driver"}],
            "pairings": [{"led": "XHP70.3 HI", "driver": "Boost driver"}],
            "specs": {"length_mm": 131, "weight_g": 110, "material": "aluminum", "max_lumens": 5200},
            "price": "31.99", "source_url": "https://sofirnlight.com/products/sc33",
        },
        confidence_score=0.9, confidence_tier="high",
        extraction_prompt_version="v1",
        extracted_at=datetime.now(timezone.utc),
    )


def fake_repo(raw_pages: list[RawPage] = None, crawl_run: CrawlRun = None):
    repo = MagicMock()
    repo.create_crawl_run.return_value = crawl_run or make_crawl_run()
    repo.complete_crawl_run.return_value = crawl_run or make_crawl_run()
    repo.save_raw_page.side_effect = lambda **kw: make_raw_page()
    repo.get_raw_pages_for_run.return_value = raw_pages or [make_raw_page()]
    repo.save_extracted_product.side_effect = lambda **kw: make_extracted_product()
    repo.log_promotion.return_value = {}
    return repo


def fake_page_crawler(pages: list[RawPage] = None):
    crawler = MagicMock()
    pages = pages or [make_raw_page()]
    crawler.crawl_product = AsyncMock(side_effect=pages)
    return crawler


def fake_spec_extractor(product: ExtractedProduct = None):
    extractor = MagicMock()
    extractor.extract.return_value = product or make_extracted_product()
    return extractor


def fake_promotion_engine(action: str = "insert"):
    from src.promotion.promotion_engine import PromotionResult
    from src.extractor.configuration_graph_builder import Configuration
    engine = MagicMock()
    config = Configuration(led="XHP70.3 HI", driver="Boost driver")
    engine.promote.return_value = [PromotionResult(action=action, configuration=config)]
    return engine


URLS = ["https://sofirnlight.com/products/sc33"]


# --- Behavior 0: ExtractionError → pipeline continues, records failure in PipelineResult ---

@pytest.mark.asyncio
async def test_extraction_error_is_recorded_and_pipeline_continues():
    from src.pipeline import ScraperPipeline
    from src.extractor.spec_extractor import ExtractionError

    page1 = make_raw_page(id=1)
    page2 = make_raw_page(id=2, crawl_run_id=1)
    page2 = RawPage(
        id=2, crawl_run_id=1,
        url="https://sofirnlight.com/products/sc31",
        markdown="SC31 specs...",
        image_urls=[], raw_variant_data=None,
        crawled_at=page1.crawled_at, scraper_version="test",
    )

    extractor = MagicMock()
    extractor.extract.side_effect = [
        ExtractionError(reason="json_parse_error", detail="Sorry I cannot..."),
        make_extracted_product(id=2, raw_page_id=2),
    ]

    repo = fake_repo(raw_pages=[page1, page2])
    crawler = fake_page_crawler(pages=[page1, page2])
    engine = fake_promotion_engine()

    pipeline = ScraperPipeline(
        page_crawler=crawler,
        spec_extractor=extractor,
        promotion_engine=engine,
        repo=repo,
    )

    result = await pipeline.run(phase="both", urls=URLS + ["https://sofirnlight.com/products/sc31"])

    # One succeeded, one failed
    assert result.products_extracted == 1
    assert len(result.failed_extractions) == 1
    assert result.failed_extractions[0].url == "https://sofirnlight.com/products/sc33"
    assert result.failed_extractions[0].reason == "json_parse_error"
    assert "Sorry I cannot" in result.failed_extractions[0].detail


# --- Behavior 1: phase='crawl' calls PageCrawler, does NOT call SpecExtractor ---

@pytest.mark.asyncio
async def test_crawl_phase_calls_crawler_not_extractor():
    from src.pipeline import ScraperPipeline

    crawler = fake_page_crawler()
    extractor = fake_spec_extractor()
    repo = fake_repo()
    engine = fake_promotion_engine()

    pipeline = ScraperPipeline(
        page_crawler=crawler,
        spec_extractor=extractor,
        promotion_engine=engine,
        repo=repo,
    )

    result = await pipeline.run(phase="crawl", urls=URLS)

    assert result.pages_crawled == 1
    assert result.products_extracted == 0
    crawler.crawl_product.assert_called_once()
    extractor.extract.assert_not_called()


# --- Behavior 2: phase='extract' reads raw_pages from repo, does NOT crawl ---

@pytest.mark.asyncio
async def test_extract_phase_reads_repo_not_crawler():
    from src.pipeline import ScraperPipeline

    crawler = fake_page_crawler()
    extractor = fake_spec_extractor()
    repo = fake_repo(raw_pages=[make_raw_page(id=1), make_raw_page(id=2)])
    engine = fake_promotion_engine()

    pipeline = ScraperPipeline(
        page_crawler=crawler,
        spec_extractor=extractor,
        promotion_engine=engine,
        repo=repo,
    )

    result = await pipeline.run(phase="extract")

    assert result.pages_crawled == 0
    assert result.products_extracted == 2
    crawler.crawl_product.assert_not_called()
    assert extractor.extract.call_count == 2


# --- Behavior 3: phase='both' crawls then extracts in sequence ---

@pytest.mark.asyncio
async def test_both_phases_crawl_then_extract():
    from src.pipeline import ScraperPipeline

    crawler = fake_page_crawler(pages=[make_raw_page(id=1), make_raw_page(id=2)])
    extractor = fake_spec_extractor()
    repo = fake_repo(raw_pages=[make_raw_page(id=1), make_raw_page(id=2)])
    engine = fake_promotion_engine()

    pipeline = ScraperPipeline(
        page_crawler=crawler,
        spec_extractor=extractor,
        promotion_engine=engine,
        repo=repo,
    )

    result = await pipeline.run(phase="both", urls=URLS + ["https://sofirnlight.com/products/sp36"])

    assert result.pages_crawled == 2
    assert result.products_extracted == 2
    assert crawler.crawl_product.call_count == 2
    assert extractor.extract.call_count == 2


# --- Behavior 4: promote=True triggers PromotionEngine after extraction ---

@pytest.mark.asyncio
async def test_promote_flag_triggers_promotion_engine():
    from src.pipeline import ScraperPipeline

    crawler = fake_page_crawler()
    extractor = fake_spec_extractor()
    repo = fake_repo()
    engine = fake_promotion_engine(action="insert")

    pipeline = ScraperPipeline(
        page_crawler=crawler,
        spec_extractor=extractor,
        promotion_engine=engine,
        repo=repo,
    )

    result = await pipeline.run(phase="both", urls=URLS, promote=True)

    engine.promote.assert_called_once()
    assert len(result.promotion_results) == 1
    assert result.promotion_results[0].action == "insert"


# --- Behavior 5: dry_run=True produces result but zero writes ---

@pytest.mark.asyncio
async def test_dry_run_makes_no_writes():
    from src.pipeline import ScraperPipeline

    crawler = fake_page_crawler()
    extractor = fake_spec_extractor()
    repo = fake_repo()
    engine = fake_promotion_engine()

    pipeline = ScraperPipeline(
        page_crawler=crawler,
        spec_extractor=extractor,
        promotion_engine=engine,
        repo=repo,
    )

    result = await pipeline.run(phase="both", urls=URLS, promote=True, dry_run=True)

    # Still reports what would have happened
    assert result.pages_crawled == 1

    # Nothing persisted: no complete_crawl_run, no save_extracted_product, no log_promotion
    repo.complete_crawl_run.assert_not_called()
    repo.save_extracted_product.assert_not_called()
    engine.promote.assert_not_called()
