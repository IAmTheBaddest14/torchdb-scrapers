"""Fetch a real Sofirn product page and save as test fixture."""
import asyncio
import re
import json
import sys
import os
from pathlib import Path

# Fix Windows console encoding
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')
os.environ['PYTHONIOENCODING'] = 'utf-8'

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig


async def main():
    browser_config = BrowserConfig(
        headless=True,
        viewport_width=1920,
        viewport_height=1080,
    )
    run_config = CrawlerRunConfig(
        page_timeout=30000,
        remove_overlay_elements=True,
    )

    fixtures_dir = Path(__file__).parent.parent / "tests" / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Step 1: Discover product URLs from collection page
        print("Crawling collection page...")
        result = await crawler.arun(
            'https://sofirnlight.com/collections/sofirn-flashlights',
            config=run_config
        )
        print(f"Collection success: {result.success}, HTML: {len(result.html)} chars")

        # Find product links
        links = result.links.get('internal', [])
        product_links = [
            l['href'] for l in links
            if '/products/' in l.get('href', '')
            and l.get('href', '').count('/') <= 4
            and not any(x in l.get('href', '').lower() for x in ['battery', 'charger', 'accessory'])
        ]
        print(f"Product links found: {product_links[:5]}")

        if not product_links:
            # Try extracting from HTML directly
            urls = re.findall(r'href=["\'](/products/[a-z0-9\-]+)["\']', result.html)
            product_links = list(dict.fromkeys(urls))
            print(f"From HTML regex: {product_links[:5]}")

        if not product_links:
            print("No product links found from collection. Trying known URLs...")
            product_links = [
                '/products/sc21-pro',
                '/products/sp36-pro',
                '/products/hs41',
            ]

        # Step 2: Crawl first product page
        for path in product_links[:3]:
            url = f"https://sofirnlight.com{path}" if path.startswith('/') else path
            print(f"\nCrawling product: {url}")
            prod_result = await crawler.arun(url, config=run_config)
            print(f"  Success: {prod_result.success}, HTML: {len(prod_result.html)} chars")

            if prod_result.success and len(prod_result.html) > 10000:
                # Save HTML fixture
                html_path = fixtures_dir / "sofirn_product_page.html"
                html_path.write_text(prod_result.html, encoding='utf-8')
                print(f"  Saved HTML to {html_path}")

                # Save markdown fixture
                md_path = fixtures_dir / "sofirn_product_page.md"
                md_path.write_text(prod_result.markdown or '', encoding='utf-8')
                print(f"  Saved markdown to {md_path}")

                # Look for embedded product JSON
                print("\n  Searching for embedded product JSON...")
                for pattern in [
                    r'"options"\s*:\s*\[',
                    r'"variants"\s*:\s*\[',
                    r'option1',
                    r'"LED"',
                    r'"Emitter"',
                ]:
                    m = re.search(pattern, prod_result.html)
                    if m:
                        ctx = prod_result.html[max(0, m.start()-50):m.start()+200]
                        print(f"  Pattern '{pattern}' found: ...{ctx}...")

                # Check image URLs
                images = prod_result.media.get('images', [])
                print(f"\n  Images found: {len(images)}")
                for img in images[:3]:
                    print(f"    {img.get('src', '')[:80]}")

                print(f"\n  Markdown preview:\n{(prod_result.markdown or '')[:500]}")
                break


if __name__ == '__main__':
    asyncio.run(main())
