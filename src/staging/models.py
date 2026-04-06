from datetime import datetime
from typing import Any
from pydantic import BaseModel


class CrawlRun(BaseModel):
    id: int
    brand: str
    started_at: datetime
    completed_at: datetime | None
    pages_crawled: int | None
    scraper_version: str


class RawPage(BaseModel):
    id: int
    crawl_run_id: int
    url: str
    markdown: str | None
    image_urls: list[str]
    raw_variant_data: dict[str, Any] | None
    crawled_at: datetime
    scraper_version: str


class ExtractedProduct(BaseModel):
    id: int
    raw_page_id: int
    brand: str
    model: str
    configuration_graph: dict[str, Any]
    confidence_score: float
    confidence_tier: str
    extraction_prompt_version: str
    extracted_at: datetime


class PromotionLogEntry(BaseModel):
    id: int
    extracted_product_id: int
    torchdb_entity_type: str | None
    torchdb_entity_id: int | None
    action: str
    promoted_at: datetime
    promoted_by: str | None
    diff_summary: str | None
