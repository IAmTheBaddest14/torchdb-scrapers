"""ConfigurationGraphBuilder — applies splitting rules to produce promotable Configurations."""
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Configuration:
    led: str
    driver: str
    max_lumens: int | None = None
    cct_options: list[str] = field(default_factory=list)
    mode_group: str | None = None
    reflector: str | None = None
    length_mm: float | None = None
    weight_g: float | None = None
    material: str | None = None
    price: str | None = None
    source_url: str | None = None


class ConfigurationGraphBuilder:
    def build(self, graph: dict[str, Any]) -> list[Configuration]:
        """Apply splitting rules to a ConfigurationGraph and return promotable Configurations.

        Splitting rules:
        - Different LED emitter → separate Configuration
        - Different driver → separate Configuration
        - Different CCT for same LED → cct_options metadata, NOT a new Configuration
        - Different mode group → mode_group metadata, NOT a new Configuration
        - Different reflector → reflector metadata, NOT a new Configuration
        """
        specs = graph.get("specs") or {}
        leds_by_name = {led["name"]: led for led in graph.get("leds", [])}
        configs: list[Configuration] = []

        # Deduplicate by (led, driver) — mode group / reflector variants collapse into metadata
        seen: dict[tuple[str, str], Configuration] = {}

        for pairing in graph.get("pairings", []):
            led_name = pairing["led"].strip().lower()
            driver_name = pairing["driver"].strip().lower()
            key = (led_name, driver_name)

            if key in seen:
                # Merge metadata from additional pairings
                existing = seen[key]
                mode_group = pairing.get("mode_group")
                if mode_group and existing.mode_group and mode_group != existing.mode_group:
                    existing.mode_group = f"{existing.mode_group}, {mode_group}"
                elif mode_group and not existing.mode_group:
                    existing.mode_group = mode_group
                continue

            led = leds_by_name.get(pairing["led"], leds_by_name.get(led_name, {}))
            cct_options = list(led.get("cct_hints", []))

            seen[key] = Configuration(
                led=led_name,
                driver=driver_name,
                max_lumens=specs.get("max_lumens"),
                cct_options=cct_options,
                mode_group=pairing.get("mode_group"),
                reflector=pairing.get("reflector"),
                length_mm=specs.get("length_mm"),
                weight_g=specs.get("weight_g"),
                material=specs.get("material"),
                price=graph.get("price"),
                source_url=graph.get("source_url"),
            )

        return list(seen.values())
