from pathlib import Path
from typing import Any
import yaml
from pydantic import BaseModel, field_validator

_CONFIG_DIR = Path(__file__).parent.parent.parent / "config" / "brands"

VALID_PLATFORMS = {"shopify", "custom"}


class BrandConfigError(Exception):
    pass


class BrandConfig(BaseModel):
    brand: str
    base_url: str
    platform: str
    collection_paths: list[str]
    exclude_patterns: list[str]
    scraper_hints: dict[str, Any] = {}

    @field_validator("platform")
    @classmethod
    def platform_must_be_valid(cls, v: str) -> str:
        if v not in VALID_PLATFORMS:
            raise ValueError(f"platform must be one of {VALID_PLATFORMS}, got '{v}'")
        return v

    @classmethod
    def load(cls, brand: str) -> "BrandConfig":
        path = _CONFIG_DIR / f"{brand}.yaml"
        if not path.exists():
            raise BrandConfigError(
                f"No config found for brand '{brand}'. "
                f"Expected file at: {path}"
            )
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as e:
            raise BrandConfigError(f"Invalid YAML in config for '{brand}': {e}") from e

        missing = [f for f in ("brand", "base_url", "platform", "collection_paths", "exclude_patterns") if f not in raw]
        if missing:
            raise BrandConfigError(
                f"Config for '{brand}' is missing required fields: {missing}"
            )

        try:
            return cls(**{k: raw[k] for k in cls.model_fields if k in raw})
        except Exception as e:
            raise BrandConfigError(f"Invalid config for '{brand}': {e}") from e
