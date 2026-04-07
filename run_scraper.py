"""CLI entry point — orchestrates the full two-phase scrape pipeline."""
import argparse
import asyncio
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()


def build_pipeline(brand: str, dry_run: bool, promote: bool):
    from anthropic import Anthropic
    from src.config.brand_config import BrandConfig
    from src.crawler.page_crawler import PageCrawler
    from src.extractor.spec_extractor import SpecExtractor
    from src.extractor.ui_extractor import UIExtractor
    from src.extractor.configuration_graph_builder import ConfigurationGraphBuilder
    from src.promotion.promotion_engine import PromotionEngine
    from src.staging.repository import StagingRepository
    from src.pipeline import ScraperPipeline

    config = BrandConfig.load(brand)
    supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])
    repo = StagingRepository(supabase)
    anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    page_crawler = PageCrawler(config)
    spec_extractor = SpecExtractor(anthropic)
    promotion_engine = PromotionEngine(
        torchdb_client=None,  # stub until TorchDB schema (#5) is implemented
        repo=repo,
        dry_run=dry_run,
    )

    return ScraperPipeline(
        page_crawler=page_crawler,
        spec_extractor=spec_extractor,
        promotion_engine=promotion_engine,
        repo=repo,
    )


async def main():
    parser = argparse.ArgumentParser(description="TorchDB scraper pipeline")
    parser.add_argument("--brand", default="sofirn", help="Brand to scrape")
    parser.add_argument("--phase", choices=["crawl", "extract", "both"], default="both")
    parser.add_argument("--urls", help="Comma-separated product URLs (crawl phase only)")
    parser.add_argument("--promote", action="store_true", help="Run PromotionEngine after extraction")
    parser.add_argument("--dry-run", action="store_true", help="No writes to Supabase or TorchDB")
    args = parser.parse_args()

    urls = [u.strip() for u in args.urls.split(",")] if args.urls else None
    pipeline = build_pipeline(args.brand, dry_run=args.dry_run, promote=args.promote)

    result = await pipeline.run(
        phase=args.phase,
        urls=urls,
        promote=args.promote,
        dry_run=args.dry_run,
        brand=args.brand,
    )

    print(f"Pages crawled:      {result.pages_crawled}")
    print(f"Products extracted: {result.products_extracted}")
    print(f"Extraction failures:{len(result.failed_extractions)}")
    print(f"Promotion results:  {len(result.promotion_results)}")
    for pr in result.promotion_results:
        print(f"  [{pr.action}] {pr.configuration.led} / {pr.configuration.driver}"
              + (f" — {pr.diff_summary}" if pr.diff_summary else ""))
    for f in result.failed_extractions:
        print(f"  [FAILED] {f.url}\n    Reason: {f.reason}\n    Detail: {f.detail[:200]}")


if __name__ == "__main__":
    asyncio.run(main())
