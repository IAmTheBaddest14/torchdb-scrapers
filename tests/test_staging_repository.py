"""
Tests for StagingRepository — verifies behavior through public interface only.
Each test proves one piece of observable behavior against the real Supabase test schema.
"""


# --- Behavior 1: Create and retrieve a crawl run ---

def test_create_crawl_run_returns_crawl_run_with_id(repo):
    run = repo.create_crawl_run(brand="sofirn", scraper_version="1.0.0")
    assert run.id is not None
    assert run.brand == "sofirn"
    assert run.scraper_version == "1.0.0"
    assert run.completed_at is None
    assert run.pages_crawled is None


# --- Behavior 2: Save a raw page and retrieve by crawl_run_id ---

def test_save_raw_page_retrievable_by_crawl_run(repo):
    run = repo.create_crawl_run(brand="sofirn", scraper_version="1.0.0")
    page = repo.save_raw_page(
        crawl_run_id=run.id,
        url="https://sofirnlight.com/products/sp36-pro",
        markdown="# SP36 Pro\nLumens: 8000",
        image_urls=["https://cdn.sofirn.com/sp36.jpg"],
        raw_variant_data={"options": ["SST-40", "LH351D"]},
        scraper_version="1.0.0",
    )
    pages = repo.get_raw_pages_for_run(run.id)
    assert len(pages) == 1
    assert pages[0].id == page.id
    assert pages[0].url == "https://sofirnlight.com/products/sp36-pro"
    assert pages[0].markdown == "# SP36 Pro\nLumens: 8000"
    assert pages[0].image_urls == ["https://cdn.sofirn.com/sp36.jpg"]
    assert pages[0].raw_variant_data == {"options": ["SST-40", "LH351D"]}


# --- Behavior 3: Complete a crawl run updates completed_at and pages_crawled ---

def test_complete_crawl_run_sets_completed_at_and_page_count(repo):
    run = repo.create_crawl_run(brand="sofirn", scraper_version="1.0.0")
    assert run.completed_at is None
    assert run.pages_crawled is None

    completed = repo.complete_crawl_run(run.id, pages_crawled=42)
    assert completed.completed_at is not None
    assert completed.pages_crawled == 42


# --- Behavior 4: Query extracted products by confidence tier ---

def _seed_extracted_product(repo, tier: str) -> tuple:
    run = repo.create_crawl_run(brand="sofirn", scraper_version="1.0.0")
    page = repo.save_raw_page(
        crawl_run_id=run.id, url=f"https://example.com/{tier}",
        markdown="# Test", image_urls=[], raw_variant_data=None, scraper_version="1.0.0",
    )
    product = repo.save_extracted_product(
        raw_page_id=page.id, brand="sofirn", model="SP36 Pro",
        configuration_graph={"leds": ["SST-40"]},
        confidence_score=0.9 if tier == "high" else 0.5 if tier == "medium" else 0.3,
        confidence_tier=tier,
        extraction_prompt_version="v1",
    )
    return run, page, product


def test_get_extracted_products_filters_by_confidence_tier(repo):
    _seed_extracted_product(repo, "high")
    _seed_extracted_product(repo, "medium")
    _seed_extracted_product(repo, "low")

    high = repo.get_extracted_products(confidence_tier="high")
    medium = repo.get_extracted_products(confidence_tier="medium")

    assert len(high) == 1 and high[0].confidence_tier == "high"
    assert len(medium) == 1 and medium[0].confidence_tier == "medium"


# --- Behavior 5: Query extracted products by brand ---

def test_get_extracted_products_filters_by_brand(repo):
    run = repo.create_crawl_run(brand="sofirn", scraper_version="1.0.0")
    page = repo.save_raw_page(
        crawl_run_id=run.id, url="https://sofirnlight.com/products/sp36-pro",
        markdown="# SP36", image_urls=[], raw_variant_data=None, scraper_version="1.0.0",
    )
    repo.save_extracted_product(
        raw_page_id=page.id, brand="sofirn", model="SP36 Pro",
        configuration_graph={}, confidence_score=0.9,
        confidence_tier="high", extraction_prompt_version="v1",
    )

    run2 = repo.create_crawl_run(brand="wurkkos", scraper_version="1.0.0")
    page2 = repo.save_raw_page(
        crawl_run_id=run2.id, url="https://wurkkos.com/products/ts10",
        markdown="# TS10", image_urls=[], raw_variant_data=None, scraper_version="1.0.0",
    )
    repo.save_extracted_product(
        raw_page_id=page2.id, brand="wurkkos", model="TS10",
        configuration_graph={}, confidence_score=0.85,
        confidence_tier="high", extraction_prompt_version="v1",
    )

    sofirn_results = repo.get_extracted_products(brand="sofirn")
    wurkkos_results = repo.get_extracted_products(brand="wurkkos")

    assert len(sofirn_results) == 1 and sofirn_results[0].brand == "sofirn"
    assert len(wurkkos_results) == 1 and wurkkos_results[0].brand == "wurkkos"


# --- Behavior 6: Promotion log records action and timestamp ---

def test_log_promotion_records_action_and_timestamp(repo):
    _, _, product = _seed_extracted_product(repo, "high")

    entry = repo.log_promotion(
        extracted_product_id=product.id,
        action="insert",
        torchdb_entity_type="configuration",
        torchdb_entity_id=42,
        promoted_by="auto",
        diff_summary=None,
    )

    assert entry.id is not None
    assert entry.action == "insert"
    assert entry.extracted_product_id == product.id
    assert entry.torchdb_entity_type == "configuration"
    assert entry.torchdb_entity_id == 42
    assert entry.promoted_at is not None
