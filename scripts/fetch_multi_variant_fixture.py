"""Fetch a Sofirn product with multiple meaningful options (LED, CCT) as fixture."""
import asyncio
import re
import json
import sys
import os
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig


async def main():
    browser_config = BrowserConfig(headless=True, viewport_width=1920, viewport_height=1080)
    run_config = CrawlerRunConfig(page_timeout=30000, remove_overlay_elements=True)
    fixtures_dir = Path(__file__).parent.parent / "tests" / "fixtures"

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Try products likely to have LED/CCT options
        candidates = [
            'https://www.sofirnlight.com/products/sofirn-hs41-rechargeable-headlamp',
            'https://www.sofirnlight.com/products/sofirn-sc33-edc-flashlight-5200lm',
            'https://www.sofirnlight.com/products/sofirn-sp36-bls',
            'https://www.sofirnlight.com/products/sofirn-if23-pro-5000-lumens-edc-powerful-flashlight',
        ]

        for url in candidates:
            print(f"Trying: {url}")
            result = await crawler.arun(url, config=run_config)
            if not result.success or len(result.html) < 10000:
                print(f"  Skipping - success:{result.success} html:{len(result.html)}")
                continue

            # Extract embedded product JSON
            m = re.search(r'"options"\s*:\s*(\[.*?\])\s*,\s*"variants"', result.html, re.DOTALL)
            if m:
                try:
                    options = json.loads(m.group(1))
                    option_names = [o['name'] for o in options]
                    print(f"  Options: {option_names}")
                    all_values = {o['name']: o['values'] for o in options}
                    print(f"  Values: {all_values}")

                    # Look for LED/CCT/Driver options
                    interesting = any(
                        any(k in name.lower() for k in ['led', 'emitter', 'cct', 'color', 'driver', 'tint'])
                        for name in option_names
                    )
                    if interesting or len(options) > 1:
                        print(f"  INTERESTING - saving as multi-variant fixture")
                        out = fixtures_dir / "sofirn_multi_variant_page.html"
                        out.write_text(result.html, encoding='utf-8')
                        md_out = fixtures_dir / "sofirn_multi_variant_page.md"
                        md_out.write_text(result.markdown or '', encoding='utf-8')
                        print(f"  Saved to {out}")

                        # Also print full product JSON for inspection
                        prod_m = re.search(
                            r'\{[^{]*"handle"\s*:\s*"[^"]+[^{]*"options"\s*:\s*\[.*?"variants"\s*:\s*\[.*?\]\s*\}',
                            result.html, re.DOTALL
                        )
                        if prod_m:
                            snippet = prod_m.group(0)[:2000]
                            print(f"  Product JSON snippet:\n{snippet}")
                        break
                except Exception as e:
                    print(f"  Parse error: {e}")


if __name__ == '__main__':
    asyncio.run(main())
