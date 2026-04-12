"""URL file I/O and interactive selection utilities for the CLI."""
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


def load_urls_from_file(path: str | Path) -> list[str]:
    """Load URLs from a JSON envelope or plain-text file (one URL per line)."""
    path = Path(path)
    text = path.read_text(encoding="utf-8")

    # Try JSON first
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "urls" in data:
            return data["urls"]
        if isinstance(data, list):
            return data
    except json.JSONDecodeError:
        pass

    # Fall back to plain text — one URL per line, skip blanks
    return [line.strip() for line in text.splitlines() if line.strip()]


def save_discovered_urls(brand: str, urls: list[str], output_dir: str | Path = None) -> Path:
    """Write discovered URLs to data/discover/{brand}_urls.json.

    Returns the path of the written file.
    """
    if output_dir is None:
        output_dir = Path("data") / "discover"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    path = output_dir / f"{brand}_urls.json"
    data = {
        "brand": brand,
        "discovered_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "urls": urls,
    }
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return path


def select_urls_interactively(urls: list[str], input_fn=input) -> list[str]:
    """Present a numbered list of URLs and return the user's selection.

    Accepts comma-separated numbers (1-based) or 'all'.
    """
    print(f"\nDiscovered {len(urls)} URLs:")
    for i, url in enumerate(urls, start=1):
        slug = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
        print(f"  [{i}]  {slug}")

    raw = input_fn("\nEnter numbers to crawl (comma-separated) or 'all': ").strip()

    if raw.lower() == "all":
        return urls

    selected = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            idx = int(part) - 1
            if 0 <= idx < len(urls):
                selected.append(urls[idx])
    return selected
