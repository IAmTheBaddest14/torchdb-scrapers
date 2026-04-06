"""
Tests for BrandConfig — verifies behavior through public interface only.
No assertions on internal YAML parsing details.
"""
import pytest
from src.config.brand_config import BrandConfig, BrandConfigError


# --- Behavior 1: Load Sofirn returns correct core fields ---

def test_load_sofirn_returns_correct_core_fields():
    config = BrandConfig.load("sofirn")
    assert config.brand == "sofirn"
    assert config.base_url == "https://sofirnlight.com"
    assert config.platform == "shopify"


# --- Behavior 2: Non-existent brand raises descriptive error ---

def test_load_unknown_brand_raises_brand_config_error():
    with pytest.raises(BrandConfigError, match="No config found for brand 'unknownbrand'"):
        BrandConfig.load("unknownbrand")


# --- Behavior 3: YAML missing required field raises descriptive error ---

def test_load_config_missing_required_field_raises_error(tmp_path, monkeypatch):
    # Point config loader at a temp dir with a broken YAML
    incomplete = tmp_path / "testbrand.yaml"
    incomplete.write_text("brand: testbrand\nbase_url: https://example.com\n")  # missing platform etc.

    import src.config.brand_config as mod
    monkeypatch.setattr(mod, "_CONFIG_DIR", tmp_path)

    with pytest.raises(BrandConfigError, match="missing required fields"):
        BrandConfig.load("testbrand")


# --- Behavior 4: Sofirn exclude_patterns are present and non-empty ---

def test_sofirn_has_non_empty_exclude_patterns():
    config = BrandConfig.load("sofirn")
    assert len(config.exclude_patterns) > 0


# --- Behavior 5: Both shopify and custom platform types parse without error ---

def test_custom_platform_parses_without_error(tmp_path, monkeypatch):
    valid = tmp_path / "custombrand.yaml"
    valid.write_text(
        "brand: custombrand\n"
        "base_url: https://example.com\n"
        "platform: custom\n"
        "collection_paths:\n  - /products\n"
        "exclude_patterns:\n  - battery\n"
    )
    import src.config.brand_config as mod
    monkeypatch.setattr(mod, "_CONFIG_DIR", tmp_path)

    config = BrandConfig.load("custombrand")
    assert config.platform == "custom"


def test_invalid_platform_raises_error(tmp_path, monkeypatch):
    invalid = tmp_path / "badbrand.yaml"
    invalid.write_text(
        "brand: badbrand\n"
        "base_url: https://example.com\n"
        "platform: magento\n"
        "collection_paths:\n  - /products\n"
        "exclude_patterns:\n  - battery\n"
    )
    import src.config.brand_config as mod
    monkeypatch.setattr(mod, "_CONFIG_DIR", tmp_path)

    with pytest.raises(BrandConfigError, match="Invalid config"):
        BrandConfig.load("badbrand")
