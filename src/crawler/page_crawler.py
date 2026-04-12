"""PageCrawler — fetches product pages via Crawl4AI and saves to staging."""
import base64
import re
from urllib.parse import urlparse, urljoin

import fitz  # PyMuPDF
import httpx
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from src.config.brand_config import BrandConfig
from src.crawler.page_parser import parse_variant_options, parse_variants, parse_image_urls
from src.staging.models import RawPage
from src.staging.repository import StagingRepository

_SCRAPER_VERSION = "0.1.0"
_PDF_LINK_RE = re.compile(r'(?:href|src)=["\']([^"\']*\.pdf(?:\?[^"\']*)?)["\']', re.IGNORECASE)
_MANUAL_URL_RE = re.compile(r"manual|guide|instruction", re.IGNORECASE)

_DEFAULT_UI_KEYWORDS = [
    "General Operation",
    "Operation Guide",
    "How to Use",
    "Operating Instructions",
    "Button Operation",
    "Usage Instructions",
    "User Interface",
]

# Minimum page text length to skip likely TOC/cover pages
_MIN_PAGE_TEXT_LEN = 200

# Render DPI for the final UI diagram image.
# PDFs are vector-based so the viewer renders crisply at any zoom.
# 200 DPI gives print-quality sharpness for zoomed inspection.
# (72 DPI = 1:1 pt-to-px, so 200 DPI = scale factor of 200/72 ≈ 2.78)
_RENDER_DPI = 300

# Width for page selection thumbnails.
# Landscape pages need more pixels to be legible — scale by aspect ratio.
_THUMBNAIL_TARGET_AREA = 600 * 400  # ~240K pixels regardless of orientation

# Width for crop-point detection render (enough detail to read layout)
_CROP_RENDER_WIDTH = 1000


class PageCrawler:
    def __init__(self, config: BrandConfig, vision_client=None, scraper_version: str = _SCRAPER_VERSION):
        self.config = config
        self._vision_client = vision_client
        self.scraper_version = scraper_version

    def _ui_keywords(self) -> list[str]:
        """Return UI diagram keywords from brand config, falling back to defaults."""
        return self.config.ui_diagram.get("keywords", _DEFAULT_UI_KEYWORDS)

    def _page_index_hint(self) -> int | None:
        """Return optional page index hint from brand config."""
        return self.config.ui_diagram.get("page_index_hint")

    async def crawl_product(
        self,
        url: str,
        crawl_run_id: int,
        repo: StagingRepository,
    ) -> RawPage | None:
        """Crawl a product page and save the result as a RawPage in staging.

        Returns the saved RawPage, or None if the crawl failed.
        """
        browser_config = BrowserConfig(headless=True, viewport_width=1920, viewport_height=1080)
        run_config = CrawlerRunConfig(page_timeout=30000, remove_overlay_elements=True)

        async with AsyncWebCrawler(config=browser_config) as crawler:
            result = await crawler.arun(url, config=run_config)

        if not result.success or len(result.html or "") < 5000:
            return None

        html = result.html
        image_urls = parse_image_urls(html)
        options = parse_variant_options(html)
        variants = parse_variants(html)

        raw_variant_data: dict = {}
        if options or variants:
            raw_variant_data = {"options": options, "variants": variants}

        manual_pdf_url, manual_pdf_text, manual_ui_diagram_url = await self._fetch_pdf(html, url, repo)

        return repo.save_raw_page(
            crawl_run_id=crawl_run_id,
            url=url,
            markdown=result.markdown,
            image_urls=image_urls,
            raw_variant_data=raw_variant_data or None,
            scraper_version=self.scraper_version,
            manual_pdf_url=manual_pdf_url,
            manual_pdf_text=manual_pdf_text,
            manual_ui_diagram_url=manual_ui_diagram_url,
        )

    async def _fetch_pdf(
        self, html: str, page_url: str, repo: StagingRepository
    ) -> tuple[str | None, str | None, str | None]:
        """Find, download, and upload the product manual PDF.

        Returns (hosted_pdf_url, pdf_text, ui_diagram_url).
        """
        pdf_url = self._find_pdf_url(html, page_url)
        if not pdf_url:
            return None, None, None

        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
                response = await client.get(pdf_url)
                response.raise_for_status()
                pdf_bytes = response.content
        except Exception as e:
            print(f"  [PDF] Download failed for {pdf_url}: {e}")
            return None, None, None

        slug = urlparse(page_url).path.rstrip("/").split("/")[-1]

        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            pdf_text = "\n".join(
                page.get_text() for page in doc
            ).strip()
        except Exception as e:
            print(f"  [PDF] Text extraction failed: {e}")
            pdf_text = None
            doc = None

        try:
            hosted_url = repo.upload_pdf(self.config.brand, slug, pdf_bytes)
            print(f"  [PDF] Uploaded → {hosted_url}")
        except Exception as e:
            print(f"  [PDF] Upload failed: {e}")
            hosted_url = pdf_url  # fall back to manufacturer URL

        ui_diagram_url = self._extract_ui_diagram(doc, slug, repo) if doc is not None else None

        return hosted_url, pdf_text, ui_diagram_url

    def _render_page_as_b64(self, page: fitz.Page, width: int | None = None, target_area: int | None = None) -> str:
        """Render a PDF page and return as a base64 PNG string.

        Specify either `width` (pixel width) or `target_area` (total pixel count)
        to control output size.  target_area keeps landscape/portrait balanced.
        """
        if target_area is not None:
            aspect = page.rect.width / page.rect.height
            width = int((target_area * aspect) ** 0.5)
        scale = width / page.rect.width
        mat = fitz.Matrix(scale, scale)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        return base64.b64encode(pix.tobytes("png")).decode()

    def _select_diagram_page_vision(
        self, doc: fitz.Document, candidate_indices: list[int]
    ) -> int | None:
        """Send candidate page thumbnails to vision LLM and return the chosen page index.

        Each candidate is rendered as a small thumbnail and labeled "Page 1", "Page 2", etc.
        The LLM replies with the page number that contains the UI/operation diagram, or '0'
        if none clearly show one.  Falls back to the last candidate on LLM failure.
        """
        content = []
        for pos, idx in enumerate(candidate_indices, start=1):
            b64 = self._render_page_as_b64(doc[idx], target_area=_THUMBNAIL_TARGET_AREA)
            content.append({"type": "text", "text": f"Page {pos}:"})
            content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/png", "data": b64},
            })
        content.append({
            "type": "text",
            "text": (
                "Which page number (1, 2, 3, etc.) contains the UI/operation section — "
                "this may be a flowchart, a mode table, or illustrated figures showing button "
                "press sequences, mode changes, and brightness levels? "
                "Reply with ONLY the number. If none show UI operation content, reply '0'."
            ),
        })

        try:
            raw = self._vision_client.complete(
                system=(
                    "You are analyzing pages from a flashlight product manual. "
                    "Identify which page contains the UI/operation section — this could be a flowchart, "
                    "a table of modes and brightness levels, or illustrated figures showing how to "
                    "press buttons to change modes, turn on/off, or access special functions."
                ),
                messages=[{"role": "user", "content": content}],
                max_tokens=16,
            )
            num = int(raw.strip().split()[0])
            if num == 0 or num > len(candidate_indices):
                print("  [PDF] Vision: no UI diagram found among candidates")
                return None
            chosen = candidate_indices[num - 1]
            print(f"  [PDF] Vision selected page {chosen} (candidate {num} of {len(candidate_indices)})")
            return chosen
        except Exception as e:
            print(f"  [PDF] Vision page selection failed ({e}) — falling back to last candidate")
            return candidate_indices[-1]

    @staticmethod
    def _find_content_column_x(page: fitz.Page) -> float | None:
        """Return the x-coordinate where a right-side non-diagram column begins.

        Sofirn-style poster manuals have a CONTENTS index and/or English instruction
        column on the far right that is not part of the operation figures.
        Returns the x0 of that column, or None if not found.
        """
        markers = ["CONTENTS", "Contents"]
        for b in page.get_text("blocks"):
            text = b[4].strip()
            if any(text.startswith(m) for m in markers):
                return b[0]  # x0 of block
        return None

    @staticmethod
    def _find_multilang_boundary_y(page: fitz.Page) -> float | None:
        """Return the y-coordinate (in page points) where non-English language sections begin.

        Detects standalone column headers like '(DE)Deutsch' that Sofirn-style poster manuals
        use to mark the start of repeated multilingual instruction blocks.

        Filters out table-of-contents lines (long strings with fill characters) that also
        contain language codes but are not section boundaries.
        """
        lang_pattern = re.compile(
            r'\((DE|FR|ES|IT|JP|ZH|KO|PT|NL|PL|RU|AR|TR|CS|SV|DA|FI|NO|HU|RO|SK|UK)\)'
        )
        y_values = []
        for b in page.get_text("blocks"):
            text = b[4]
            if not lang_pattern.search(text):
                continue
            # Skip TOC-style entries: they're long and contain repeated fill characters
            # (dots, dashes, box-drawing chars used to pad page number columns)
            stripped = text.strip()
            if len(stripped) > 60:
                continue
            y_values.append(b[1])
        return min(y_values) if y_values else None

    def _find_crop_vision(self, page: fitz.Page) -> tuple[float, float] | None:
        """Ask vision LLM for the vertical span of the UI diagram section.

        Returns (start_pct, end_pct) as percentages from the top (0–100).
        Returns None on failure (caller falls back to text search or bottom-60% crop).

        Examples:
          (40, 100) — diagram is in the lower 60% (typical multi-page portrait manual)
          (0, 35)   — diagram is in the upper 35% (poster-format single-page manual)
        """
        b64 = self._render_page_as_b64(page, width=_CROP_RENDER_WIDTH)
        try:
            raw = self._vision_client.complete(
                system=(
                    "You are identifying the vertical location of a UI/operation diagram section "
                    "on a flashlight manual page. The diagram may be a flowchart, a table of modes, "
                    "or illustrated figures showing button press sequences and operating steps. "
                    "Exclude repeated multilingual instruction blocks (e.g. the same text in German, "
                    "French, Spanish, etc.) — stop the range just before those begin."
                ),
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": b64}},
                        {"type": "text", "text": (
                            "At approximately what percentage range from the TOP of this image does the "
                            "UI/operation diagram section span (the flowchart, mode table, or figures showing "
                            "button presses and operating steps)? "
                            "Stop before any repeated multilingual text sections.\n"
                            "Reply with ONLY two numbers separated by a dash, e.g. '40-100' or '0-28'. "
                            "Use 0-100 if the diagram spans the whole page."
                        )},
                    ],
                }],
                max_tokens=16,
            )
            numbers = re.findall(r'\d+(?:\.\d+)?', raw)
            if len(numbers) < 2:
                raise ValueError(f"expected two numbers, got: {raw!r}")
            start_pct = max(0.0, min(100.0, float(numbers[0])))
            end_pct = max(0.0, min(100.0, float(numbers[1])))
            if end_pct <= start_pct:
                end_pct = 100.0
            print(f"  [PDF] Vision crop: {start_pct:.0f}%–{end_pct:.0f}% from top")
            return start_pct, end_pct
        except Exception as e:
            print(f"  [PDF] Vision crop detection failed ({e}) — using fallback")
            return None

    def _extract_ui_diagram(
        self, doc: fitz.Document, slug: str, repo: StagingRepository
    ) -> str | None:
        """Render the operation/UI page from the PDF as a PNG and upload it.

        Two-stage approach when a vision_client is available:
          Stage 1 — text scan finds candidate pages; vision confirms the right one.
          Stage 2 — vision identifies the crop point; falls back to text search
                     then bottom-60% if vision is unavailable or fails.
        """
        keywords = self._ui_keywords()
        keyword_pattern = re.compile(
            "|".join(re.escape(k) for k in keywords), re.IGNORECASE
        )

        # Stage 1a: text scan — collect ALL candidate pages (not just the last match)
        candidate_indices: list[int] = []
        for i, page in enumerate(doc):
            text = page.get_text()
            if len(text.strip()) < _MIN_PAGE_TEXT_LEN:
                continue
            if keyword_pattern.search(text):
                candidate_indices.append(i)

        if not candidate_indices:
            page_index_hint = self._page_index_hint()
            if page_index_hint is not None:
                print(f"  [PDF] No keyword match — using page_index_hint ({page_index_hint})")
                candidate_indices = [page_index_hint]

        if not candidate_indices:
            if self._vision_client is not None:
                # No keyword matches — let vision scan all substantive pages
                all_pages = [
                    i for i, p in enumerate(doc)
                    if len(p.get_text().strip()) >= _MIN_PAGE_TEXT_LEN
                ]
                # If every page is sparse (image-heavy PDF), scan all pages
                candidate_indices = all_pages or list(range(len(doc)))
                print(f"  [PDF] No keyword match — vision will scan all {len(candidate_indices)} page(s)")
            else:
                print("  [PDF] No UI diagram page found")
                return None

        print(f"  [PDF] {len(candidate_indices)} candidate page(s): {candidate_indices}")

        # Stage 1b: vision confirms which candidate is the actual diagram page
        if self._vision_client is not None:
            target_idx = self._select_diagram_page_vision(doc, candidate_indices)
            if target_idx is None:
                return None
        else:
            target_idx = candidate_indices[-1]  # last match wins (pre-vision behaviour)

        # Stage 2: determine crop rect, then render at DPI-based resolution
        try:
            page = doc[target_idx]
            clip = None

            # Hard boundaries derived from text structure
            multilang_y = self._find_multilang_boundary_y(page)
            content_col_x = self._find_content_column_x(page)
            right_x = content_col_x if content_col_x is not None else page.rect.width

            if multilang_y is not None:
                print(f"  [PDF] Multilang boundary at y={multilang_y:.0f}pt "
                      f"({100 * multilang_y / page.rect.height:.0f}% from top)")
            if content_col_x is not None:
                print(f"  [PDF] Content column at x={content_col_x:.0f}pt "
                      f"({100 * content_col_x / page.rect.width:.0f}% from left)")

            if self._vision_client is not None:
                crop = self._find_crop_vision(page)
                if crop is not None:
                    start_pct, end_pct = crop
                    y_top = page.rect.height * (start_pct / 100.0)
                    y_bot = page.rect.height * (end_pct / 100.0)
                    if multilang_y is not None:
                        y_bot = min(y_bot, multilang_y - 4)
                    clip = fitz.Rect(0, y_top, right_x, y_bot)

            if clip is None:
                # Fallback 1: PyMuPDF text coordinate search
                for kw in keywords:
                    if re.search(re.escape(kw), page.get_text(), re.IGNORECASE):
                        hits = page.search_for(kw)
                        if hits:
                            y_top = max(0, hits[0].y0 - 8)
                            y_bot = multilang_y - 4 if multilang_y is not None else page.rect.height
                            clip = fitz.Rect(0, y_top, right_x, y_bot)
                            break

            if clip is None:
                # Fallback 2: bottom 60% of page (or up to multilang boundary)
                y_top = page.rect.height * 0.4
                y_bot = multilang_y - 4 if multilang_y is not None else page.rect.height
                clip = fitz.Rect(0, y_top, right_x, y_bot)
                print("  [PDF] Using bottom-60% fallback crop")

            # Guard against degenerate rects
            if clip.height <= 0:
                y_bot = multilang_y - 4 if multilang_y is not None else page.rect.height
                clip = fitz.Rect(0, 0, right_x, y_bot)
                print("  [PDF] Clip height was zero — reset to full-top crop")

            # Render at print-quality DPI so zoomed output is crisp.
            # PDFs are vector-based; 200 DPI (scale ≈ 2.78) matches what a PDF viewer
            # delivers at normal zoom and stays sharp when the user zooms in further.
            scale = _RENDER_DPI / 72.0
            mat = fitz.Matrix(scale, scale)
            pix = page.get_pixmap(matrix=mat, clip=clip, alpha=False)
            out_w, out_h = pix.width, pix.height
            print(f"  [PDF] Rendered crop at {out_w}x{out_h}px ({_RENDER_DPI} DPI)")
            png_bytes = pix.tobytes("png")
        except Exception as e:
            print(f"  [PDF] Page render failed: {e}")
            return None

        try:
            url = repo.upload_ui_diagram(self.config.brand, slug, png_bytes)
            print(f"  [PDF] UI diagram uploaded → {url}")
            return url
        except Exception as e:
            print(f"  [PDF] UI diagram upload failed: {e}")
            return None

    @staticmethod
    def _find_pdf_url(html: str, page_url: str) -> str | None:
        """Return the best PDF URL from the page HTML.

        Prefers URLs whose path contains 'manual', 'guide', or 'instruction'.
        Falls back to the first PDF link found.
        """
        candidates = []
        for match in _PDF_LINK_RE.finditer(html):
            raw = match.group(1)
            if raw.startswith("data:"):
                continue
            url = urljoin(page_url, raw)
            if url.startswith("//"):
                url = "https:" + url
            candidates.append(url)

        if not candidates:
            return None

        candidates.sort(
            key=lambda u: bool(_MANUAL_URL_RE.search(u)),
            reverse=True,
        )
        return candidates[0]
