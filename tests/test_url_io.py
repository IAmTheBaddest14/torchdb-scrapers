"""Tests for URL file I/O and interactive selection — verifies behavior through public interface."""
import json
import pytest
from pathlib import Path


# --- Behavior 1: load_urls_from_file reads JSON envelope ---

def test_load_urls_from_json_file(tmp_path):
    from src.cli.url_io import load_urls_from_file

    data = {
        "brand": "sofirn",
        "discovered_at": "2026-04-12T15:00:00Z",
        "urls": [
            "https://sofirnlight.com/products/sc33",
            "https://sofirnlight.com/products/sp36-pro",
        ],
    }
    f = tmp_path / "sofirn_urls.json"
    f.write_text(json.dumps(data), encoding="utf-8")

    urls = load_urls_from_file(f)

    assert urls == [
        "https://sofirnlight.com/products/sc33",
        "https://sofirnlight.com/products/sp36-pro",
    ]


# --- Behavior 2: load_urls_from_file reads plain text (one URL per line) ---

def test_load_urls_from_plain_text_file(tmp_path):
    from src.cli.url_io import load_urls_from_file

    f = tmp_path / "urls.txt"
    f.write_text(
        "https://sofirnlight.com/products/sc33\n"
        "https://sofirnlight.com/products/sp36-pro\n"
        "\n",  # trailing blank line ignored
        encoding="utf-8",
    )

    urls = load_urls_from_file(f)

    assert urls == [
        "https://sofirnlight.com/products/sc33",
        "https://sofirnlight.com/products/sp36-pro",
    ]


# --- Behavior 3: save_discovered_urls writes correct JSON to data/discover/{brand}_urls.json ---

def test_save_discovered_urls_writes_json_file(tmp_path):
    from src.cli.url_io import save_discovered_urls

    urls = [
        "https://sofirnlight.com/products/sc33",
        "https://sofirnlight.com/products/sp36-pro",
    ]

    path = save_discovered_urls("sofirn", urls, output_dir=tmp_path)

    assert path == tmp_path / "sofirn_urls.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["brand"] == "sofirn"
    assert data["urls"] == urls
    assert "discovered_at" in data


# --- Behavior 4: select_urls_interactively with "all" returns all URLs ---

def test_select_urls_interactively_all_returns_everything():
    from src.cli.url_io import select_urls_interactively

    urls = [
        "https://sofirnlight.com/products/sc33",
        "https://sofirnlight.com/products/sp36-pro",
        "https://sofirnlight.com/products/sc21-pro",
    ]

    result = select_urls_interactively(urls, input_fn=lambda _: "all")

    assert result == urls


# --- Behavior 5: select_urls_interactively with comma-separated numbers returns subset ---

def test_select_urls_interactively_by_number_returns_subset():
    from src.cli.url_io import select_urls_interactively

    urls = [
        "https://sofirnlight.com/products/sc33",
        "https://sofirnlight.com/products/sp36-pro",
        "https://sofirnlight.com/products/sc21-pro",
    ]

    result = select_urls_interactively(urls, input_fn=lambda _: "1,3")

    assert result == [
        "https://sofirnlight.com/products/sc33",
        "https://sofirnlight.com/products/sc21-pro",
    ]
