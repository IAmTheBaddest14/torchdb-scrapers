from supabase import Client
from .models import CrawlRun, RawPage, ExtractedProduct, PromotionLogEntry


class StagingRepository:
    def __init__(self, client: Client, schema: str = "scraper_staging"):
        self._db = client
        self._schema = schema

    def _table(self, name: str):
        return self._db.schema(self._schema).table(name)

    # --- Crawl Runs ---

    def create_crawl_run(self, brand: str, scraper_version: str) -> CrawlRun:
        row = (
            self._table("crawl_runs")
            .insert({"brand": brand, "scraper_version": scraper_version})
            .execute()
            .data[0]
        )
        return CrawlRun(**row)

    def complete_crawl_run(self, run_id: int, pages_crawled: int) -> CrawlRun:
        from datetime import datetime, timezone
        row = (
            self._table("crawl_runs")
            .update({"completed_at": datetime.now(timezone.utc).isoformat(), "pages_crawled": pages_crawled})
            .eq("id", run_id)
            .execute()
            .data[0]
        )
        return CrawlRun(**row)

    # --- Raw Pages ---

    def save_raw_page(
        self,
        crawl_run_id: int,
        url: str,
        markdown: str | None,
        image_urls: list[str],
        raw_variant_data: dict | None,
        scraper_version: str,
    ) -> RawPage:
        row = (
            self._table("raw_pages")
            .insert({
                "crawl_run_id": crawl_run_id,
                "url": url,
                "markdown": markdown,
                "image_urls": image_urls,
                "raw_variant_data": raw_variant_data,
                "scraper_version": scraper_version,
            })
            .execute()
            .data[0]
        )
        return RawPage(**row)

    def get_raw_pages_for_run(self, crawl_run_id: int) -> list[RawPage]:
        rows = (
            self._table("raw_pages")
            .select("*")
            .eq("crawl_run_id", crawl_run_id)
            .execute()
            .data
        )
        return [RawPage(**r) for r in rows]

    # --- Extracted Products ---

    def save_extracted_product(
        self,
        raw_page_id: int,
        brand: str,
        model: str,
        configuration_graph: dict,
        confidence_score: float,
        confidence_tier: str,
        extraction_prompt_version: str,
    ) -> ExtractedProduct:
        row = (
            self._table("extracted_products")
            .insert({
                "raw_page_id": raw_page_id,
                "brand": brand,
                "model": model,
                "configuration_graph": configuration_graph,
                "confidence_score": confidence_score,
                "confidence_tier": confidence_tier,
                "extraction_prompt_version": extraction_prompt_version,
            })
            .execute()
            .data[0]
        )
        return ExtractedProduct(**row)

    def get_extracted_products(
        self,
        brand: str | None = None,
        confidence_tier: str | None = None,
        crawl_run_id: int | None = None,
    ) -> list[ExtractedProduct]:
        if crawl_run_id:
            # Resolve raw_page_ids for this crawl run first, then filter
            page_ids = [
                p["id"]
                for p in self._table("raw_pages")
                .select("id")
                .eq("crawl_run_id", crawl_run_id)
                .execute()
                .data
            ]
            if not page_ids:
                return []
            query = self._table("extracted_products").select("*").in_("raw_page_id", page_ids)
        else:
            query = self._table("extracted_products").select("*")

        if brand:
            query = query.eq("brand", brand)
        if confidence_tier:
            query = query.eq("confidence_tier", confidence_tier)

        return [ExtractedProduct(**r) for r in query.execute().data]

    # --- Promotion Log ---

    def log_promotion(
        self,
        extracted_product_id: int,
        action: str,
        torchdb_entity_type: str | None = None,
        torchdb_entity_id: int | None = None,
        promoted_by: str | None = None,
        diff_summary: str | None = None,
    ) -> PromotionLogEntry:
        row = (
            self._table("promotion_log")
            .insert({
                "extracted_product_id": extracted_product_id,
                "action": action,
                "torchdb_entity_type": torchdb_entity_type,
                "torchdb_entity_id": torchdb_entity_id,
                "promoted_by": promoted_by,
                "diff_summary": diff_summary,
            })
            .execute()
            .data[0]
        )
        return PromotionLogEntry(**row)
