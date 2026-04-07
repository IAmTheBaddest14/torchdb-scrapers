"""
Tests for PromotionEngine — verifies promotion rules through public interface only.
Uses FakeTorchDBClient and FakeRepo — no Supabase calls needed (staging layer
already tested in test_staging_repository.py).
"""
import pytest
from datetime import datetime, timezone
from src.staging.models import ExtractedProduct
from src.extractor.configuration_graph_builder import Configuration


def make_extracted_product(
    confidence_tier: str = "high",
    confidence_score: float = 0.9,
    graph: dict = None,
) -> ExtractedProduct:
    return ExtractedProduct(
        id=1,
        raw_page_id=1,
        brand="sofirn",
        model="SC33",
        configuration_graph=graph or {
            "product_name": "Sofirn SC33",
            "brand": "sofirn",
            "leds": [{"name": "XHP70.3 HI", "cct_hints": ["6500K"]}],
            "drivers": [{"name": "Boost driver"}],
            "pairings": [{"led": "XHP70.3 HI", "driver": "Boost driver"}],
            "specs": {"length_mm": 131, "weight_g": 110, "material": "aluminum", "max_lumens": 5200},
            "price": "31.99",
            "source_url": "https://sofirnlight.com/products/sc33",
        },
        confidence_score=confidence_score,
        confidence_tier=confidence_tier,
        extraction_prompt_version="v1",
        extracted_at=datetime.now(timezone.utc),
    )


class FakeTorchDBClient:
    def __init__(self, existing: dict = None):
        self._existing = existing or {}  # (brand, led, driver) -> record dict
        self.inserted: list[dict] = []

    def find_product(self, brand: str, led: str, driver: str) -> dict | None:
        return self._existing.get((brand, led, driver))

    def insert_product(self, data: dict) -> int:
        self.inserted.append(data)
        return 100 + len(self.inserted)


class FakeRepo:
    def __init__(self):
        self.logged: list[dict] = []

    def log_promotion(self, extracted_product_id, action, torchdb_entity_type=None,
                      torchdb_entity_id=None, promoted_by=None, diff_summary=None):
        entry = dict(
            extracted_product_id=extracted_product_id,
            action=action,
            torchdb_entity_type=torchdb_entity_type,
            torchdb_entity_id=torchdb_entity_id,
            promoted_by=promoted_by,
            diff_summary=diff_summary,
        )
        self.logged.append(entry)
        return entry


# --- Behavior 1: High confidence + new record → insert ---

def test_high_confidence_new_record_inserts_to_torchdb():
    from src.promotion.promotion_engine import PromotionEngine

    client = FakeTorchDBClient(existing={})
    repo = FakeRepo()
    engine = PromotionEngine(client, repo)

    product = make_extracted_product(confidence_tier="high", confidence_score=0.9)
    results = engine.promote(product)

    assert len(results) == 1
    assert results[0].action == "insert"
    assert len(client.inserted) == 1
    assert len(repo.logged) == 1
    assert repo.logged[0]["action"] == "insert"


# --- Behavior 2: High confidence + identical existing record → skip ---

def test_high_confidence_identical_existing_record_skips():
    from src.promotion.promotion_engine import PromotionEngine

    existing_record = {
        "max_lumens": 5200,
        "length_mm": 131,
        "weight_g": 110,
        "material": "aluminum",
    }
    client = FakeTorchDBClient(existing={("sofirn", "xhp70.3 hi", "boost driver"): existing_record})
    repo = FakeRepo()
    engine = PromotionEngine(client, repo)

    product = make_extracted_product(confidence_tier="high")
    results = engine.promote(product)

    assert len(results) == 1
    assert results[0].action == "skip"
    assert len(client.inserted) == 0
    assert repo.logged[0]["action"] == "skip"


# --- Behavior 3: High confidence + changed specs → review-required + diff_summary ---

def test_high_confidence_changed_specs_requires_review_with_diff():
    from src.promotion.promotion_engine import PromotionEngine

    # TorchDB has old lumen value
    existing_record = {
        "max_lumens": 4500,  # was 4500, now 5200
        "length_mm": 131,
        "weight_g": 110,
        "material": "aluminum",
    }
    client = FakeTorchDBClient(existing={("sofirn", "xhp70.3 hi", "boost driver"): existing_record})
    repo = FakeRepo()
    engine = PromotionEngine(client, repo)

    product = make_extracted_product(confidence_tier="high")
    results = engine.promote(product)

    assert len(results) == 1
    assert results[0].action == "review-required"
    assert results[0].diff_summary is not None
    assert "5200" in results[0].diff_summary
    assert "4500" in results[0].diff_summary
    assert len(client.inserted) == 0
    assert repo.logged[0]["diff_summary"] is not None


# --- Behavior 4: Medium and low confidence always go to review queue ---

@pytest.mark.parametrize("tier,score", [("medium", 0.55), ("low", 0.2)])
def test_non_high_confidence_always_requires_review(tier, score):
    from src.promotion.promotion_engine import PromotionEngine

    # Even if a matching TorchDB record exists, low/medium still go to review
    existing_record = {"max_lumens": 5200, "length_mm": 131, "weight_g": 110, "material": "aluminum"}
    client = FakeTorchDBClient(existing={("sofirn", "xhp70.3 hi", "boost driver"): existing_record})
    repo = FakeRepo()
    engine = PromotionEngine(client, repo)

    product = make_extracted_product(confidence_tier=tier, confidence_score=score)
    results = engine.promote(product)

    assert len(results) == 1
    assert results[0].action == "review-required"
    assert len(client.inserted) == 0


# --- Behavior 5: Dry run returns correct decisions but writes nothing ---

def test_dry_run_makes_no_writes():
    from src.promotion.promotion_engine import PromotionEngine

    client = FakeTorchDBClient(existing={})
    repo = FakeRepo()
    engine = PromotionEngine(client, repo, dry_run=True)

    product = make_extracted_product(confidence_tier="high")
    results = engine.promote(product)

    # Decision is still correct
    assert results[0].action == "insert"
    # But nothing was written
    assert len(client.inserted) == 0
    assert len(repo.logged) == 0
