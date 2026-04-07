"""
Tests for ConfigurationGraphBuilder — verifies splitting rules through public interface only.
All tests are pure logic — no external dependencies.
"""
import pytest


def make_graph(
    leds: list[dict],
    drivers: list[dict],
    pairings: list[dict] = None,
    specs: dict = None,
    price: str = "29.99",
    source_url: str = "https://sofirnlight.com/products/test",
) -> dict:
    if pairings is None:
        pairings = [
            {"led": led["name"], "driver": driver["name"]}
            for led in leds
            for driver in drivers
        ]
    return {
        "product_name": "Test Light",
        "brand": "sofirn",
        "leds": leds,
        "drivers": drivers,
        "pairings": pairings,
        "specs": specs or {"length_mm": 130, "weight_g": 100, "material": "aluminum", "max_lumens": 1000},
        "price": price,
        "source_url": source_url,
    }


# --- Behavior 1: 2 LEDs + 1 driver each → 2 Configurations ---

def test_two_leds_one_driver_each_produces_two_configurations():
    from src.extractor.configuration_graph_builder import ConfigurationGraphBuilder

    graph = make_graph(
        leds=[
            {"name": "XHP70.3 HI", "cct_hints": []},
            {"name": "SFT40", "cct_hints": []},
        ],
        drivers=[{"name": "Boost driver"}],
        pairings=[
            {"led": "XHP70.3 HI", "driver": "Boost driver"},
            {"led": "SFT40", "driver": "Boost driver"},
        ],
    )

    builder = ConfigurationGraphBuilder()
    configs = builder.build(graph)

    assert len(configs) == 2
    led_names = {c.led for c in configs}
    assert "xhp70.3 hi" in led_names
    assert "sft40" in led_names


# --- Behavior 2: 1 LED + 2 CCT options → 1 Configuration with CCTs as metadata ---

def test_two_cct_options_produce_one_configuration_with_cct_metadata():
    from src.extractor.configuration_graph_builder import ConfigurationGraphBuilder

    graph = make_graph(
        leds=[{"name": "XHP70.3 HI", "cct_hints": ["6500K", "5000K"]}],
        drivers=[{"name": "Boost driver"}],
    )

    builder = ConfigurationGraphBuilder()
    configs = builder.build(graph)

    assert len(configs) == 1
    assert configs[0].led == "xhp70.3 hi"
    assert "6500K" in configs[0].cct_options
    assert "5000K" in configs[0].cct_options


# --- Behavior 3: 1 LED + 2 drivers → 2 Configurations ---

def test_two_drivers_produce_two_configurations():
    from src.extractor.configuration_graph_builder import ConfigurationGraphBuilder

    graph = make_graph(
        leds=[{"name": "SFT40", "cct_hints": []}],
        drivers=[
            {"name": "FET driver"},
            {"name": "Linear driver"},
        ],
        pairings=[
            {"led": "SFT40", "driver": "FET driver"},
            {"led": "SFT40", "driver": "Linear driver"},
        ],
    )

    builder = ConfigurationGraphBuilder()
    configs = builder.build(graph)

    assert len(configs) == 2
    driver_names = {c.driver for c in configs}
    assert "fet driver" in driver_names
    assert "linear driver" in driver_names
    # Both configs share the same LED
    assert all(c.led == "sft40" for c in configs)


# --- Behavior 4: 2 mode groups → 1 Configuration with mode_group as metadata ---

def test_two_mode_groups_produce_one_configuration_with_mode_group_metadata():
    from src.extractor.configuration_graph_builder import ConfigurationGraphBuilder

    graph = make_graph(
        leds=[{"name": "SST20", "cct_hints": []}],
        drivers=[{"name": "Buck driver"}],
        pairings=[
            {"led": "SST20", "driver": "Buck driver", "mode_group": "4 modes"},
        ],
    )
    # Simulate a second mode group variant by adding a second pairing with same LED+driver
    graph["pairings"].append(
        {"led": "SST20", "driver": "Buck driver", "mode_group": "12 modes"}
    )

    builder = ConfigurationGraphBuilder()
    configs = builder.build(graph)

    # Same LED + same driver = same Configuration regardless of mode group
    assert len(configs) == 1
    assert configs[0].led == "sst20"
    assert configs[0].driver == "buck driver"
    assert configs[0].mode_group is not None


# --- Behavior 5: Each Configuration carries all extracted specs ---

def test_configuration_carries_all_extracted_specs():
    from src.extractor.configuration_graph_builder import ConfigurationGraphBuilder

    graph = make_graph(
        leds=[{"name": "XHP70.3 HI", "cct_hints": ["6500K", "5000K"]}],
        drivers=[{"name": "Boost driver"}],
        specs={
            "length_mm": 131,
            "weight_g": 110,
            "material": "AL6061-T6 aluminum alloy",
            "max_lumens": 5200,
        },
        price="31.99",
        source_url="https://sofirnlight.com/products/sofirn-sc33",
    )

    builder = ConfigurationGraphBuilder()
    configs = builder.build(graph)

    assert len(configs) == 1
    c = configs[0]
    assert c.max_lumens == 5200
    assert c.length_mm == 131
    assert c.weight_g == 110
    assert "aluminum" in c.material.lower()
    assert c.price == "31.99"
    assert c.source_url == "https://sofirnlight.com/products/sofirn-sc33"
    assert c.cct_options == ["6500K", "5000K"]


# --- Behavior 6: LED name case variance dedups to one Configuration ---

def test_led_name_case_variance_deduplicates_to_one_configuration():
    from src.extractor.configuration_graph_builder import ConfigurationGraphBuilder

    # Same LED, different case — should dedup, not split
    graph = make_graph(
        leds=[{"name": "XHP70.3 HI", "cct_hints": []}],
        drivers=[{"name": "boost driver"}],
        pairings=[
            {"led": "XHP70.3 HI", "driver": "boost driver"},
            {"led": "xhp70.3 hi", "driver": "boost driver"},
        ],
    )

    builder = ConfigurationGraphBuilder()
    configs = builder.build(graph)

    assert len(configs) == 1
    assert configs[0].led == "xhp70.3 hi"


# --- Behavior 7: Driver name case variance dedups to one Configuration ---

def test_driver_name_case_variance_deduplicates_to_one_configuration():
    from src.extractor.configuration_graph_builder import ConfigurationGraphBuilder

    graph = make_graph(
        leds=[{"name": "sft40", "cct_hints": []}],
        drivers=[{"name": "Boost Driver"}],
        pairings=[
            {"led": "sft40", "driver": "Boost Driver"},
            {"led": "sft40", "driver": "boost driver"},
        ],
    )

    builder = ConfigurationGraphBuilder()
    configs = builder.build(graph)

    assert len(configs) == 1
    assert configs[0].driver == "boost driver"
