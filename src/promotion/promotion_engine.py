"""PromotionEngine — confidence-tiered, diff-gated promotion to TorchDB."""
from dataclasses import dataclass, field
from typing import Any

from src.extractor.configuration_graph_builder import Configuration, ConfigurationGraphBuilder
from src.staging.models import ExtractedProduct

# Fields compared when diffing against an existing TorchDB record
_DIFF_FIELDS = ("max_lumens", "length_mm", "weight_g", "material")


@dataclass
class PromotionResult:
    action: str  # 'insert' | 'skip' | 'review-required' | 'rejected'
    configuration: Configuration
    diff_summary: str | None = None
    torchdb_entity_id: int | None = None


class PromotionEngine:
    def __init__(self, torchdb_client, repo, dry_run: bool = False):
        self._client = torchdb_client
        self._repo = repo
        self._dry_run = dry_run
        self._builder = ConfigurationGraphBuilder()

    def promote(self, extracted_product: ExtractedProduct) -> list[PromotionResult]:
        """Apply promotion rules to every Configuration in the extracted product.

        Returns one PromotionResult per Configuration. In dry-run mode, decisions
        are calculated but nothing is written to TorchDB or the promotion log.
        """
        configurations = self._builder.build(extracted_product.configuration_graph)
        results: list[PromotionResult] = []

        for config in configurations:
            result = self._evaluate(config, extracted_product)
            results.append(result)

            if not self._dry_run:
                self._repo.log_promotion(
                    extracted_product_id=extracted_product.id,
                    action=result.action,
                    torchdb_entity_type="product" if result.action == "insert" else None,
                    torchdb_entity_id=result.torchdb_entity_id,
                    diff_summary=result.diff_summary,
                )

        return results

    def _evaluate(self, config: Configuration, product: ExtractedProduct) -> PromotionResult:
        tier = product.confidence_tier

        # Medium / low always go to review queue
        if tier in ("medium", "low"):
            return PromotionResult(action="review-required", configuration=config)

        # High confidence — check TorchDB
        existing = self._client.find_product(product.brand, config.led, config.driver)

        if existing is None:
            # New record — auto-promote
            entity_id = None
            if not self._dry_run:
                entity_id = self._client.insert_product(self._to_torchdb_payload(config, product))
            return PromotionResult(action="insert", configuration=config, torchdb_entity_id=entity_id)

        # Existing record — diff key fields
        diff = self._diff(config, existing)
        if diff:
            return PromotionResult(action="review-required", configuration=config, diff_summary=diff)

        return PromotionResult(action="skip", configuration=config)

    def _diff(self, config: Configuration, existing: dict[str, Any]) -> str | None:
        """Return a human-readable diff string if key fields changed, else None."""
        changes: list[str] = []
        for field_name in _DIFF_FIELDS:
            new_val = getattr(config, field_name)
            old_val = existing.get(field_name)
            if new_val is not None and old_val is not None and new_val != old_val:
                changes.append(f"{field_name}: {old_val} → {new_val}")
        return "; ".join(changes) if changes else None

    @staticmethod
    def _to_torchdb_payload(config: Configuration, product: ExtractedProduct) -> dict[str, Any]:
        return {
            "brand": product.brand,
            "model": product.model,
            "led": config.led,
            "driver": config.driver,
            "max_lumens": config.max_lumens,
            "cct_options": config.cct_options,
            "length_mm": config.length_mm,
            "weight_g": config.weight_g,
            "material": config.material,
            "price": config.price,
            "source_url": config.source_url,
        }
