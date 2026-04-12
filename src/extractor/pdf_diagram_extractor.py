"""PDFDiagramExtractor — renders the UI diagram page from a product manual PDF."""
import base64
import re
from urllib.parse import urlparse

import fitz
import httpx

_RENDER_DPI = 300
_THUMBNAIL_TARGET_AREA = 600 * 400
_CROP_RENDER_WIDTH = 1000
_MIN_PAGE_TEXT_LEN = 200

_DEFAULT_UI_KEYWORDS = [
    "General Operation",
    "Operation Guide",
    "How to Use",
    "Operating Instructions",
    "Button Operation",
    "Usage Instructions",
    "User Interface",
]


class PDFDiagramExtractor:
    def __init__(self, vision_client, image_uploader, brand: str, ui_keywords: list[str] = None, page_index_hint: int = None):
        self._client = vision_client
        self._image_uploader = image_uploader
        self._brand = brand
        self._ui_keywords = ui_keywords or _DEFAULT_UI_KEYWORDS
        self._page_index_hint = page_index_hint

    def extract(self, pdf_url: str, page_url: str) -> list[dict]:
        """Download PDF, render the UI diagram page, upload PNG, return UI instance list."""
        try:
            response = httpx.get(pdf_url, follow_redirects=True, timeout=30)
            response.raise_for_status()
            pdf_bytes = response.content
        except Exception as e:
            print(f"  [PDF] Download failed for {pdf_url}: {e}")
            return []

        slug = urlparse(page_url).path.rstrip("/").split("/")[-1]

        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as e:
            print(f"  [PDF] Could not open PDF: {e}")
            return []

        diagram_url = self._extract_ui_diagram(doc, slug)
        if not diagram_url:
            return []

        return [{
            "variant_hint": None,
            "framework": "proprietary",
            "framework_source": "pdf",
            "switchable": False,
            "diagrams": [{"label": "Operation", "diagram_url": diagram_url, "completeness": 1.0}],
            "needs_review": False,
            "tags": {},
        }]

    def _extract_ui_diagram(self, doc: fitz.Document, slug: str) -> str | None:
        keyword_pattern = re.compile(
            "|".join(re.escape(k) for k in self._ui_keywords), re.IGNORECASE
        )

        candidate_indices: list[int] = []
        for i, page in enumerate(doc):
            text = page.get_text()
            if len(text.strip()) < _MIN_PAGE_TEXT_LEN:
                continue
            if keyword_pattern.search(text):
                candidate_indices.append(i)

        if not candidate_indices:
            if self._page_index_hint is not None:
                print(f"  [PDF] No keyword match — using page_index_hint ({self._page_index_hint})")
                candidate_indices = [self._page_index_hint]

        if not candidate_indices:
            all_pages = [
                i for i, p in enumerate(doc)
                if len(p.get_text().strip()) >= _MIN_PAGE_TEXT_LEN
            ]
            candidate_indices = all_pages or list(range(len(doc)))
            print(f"  [PDF] No keyword match — vision will scan all {len(candidate_indices)} page(s)")

        print(f"  [PDF] {len(candidate_indices)} candidate page(s): {candidate_indices}")

        target_idx = self._select_diagram_page_vision(doc, candidate_indices)
        if target_idx is None:
            return None

        try:
            page = doc[target_idx]
            clip = None

            multilang_y = self._find_multilang_boundary_y(page)
            content_col_x = self._find_content_column_x(page)
            right_x = content_col_x if content_col_x is not None else page.rect.width

            crop = self._find_crop_vision(page)
            if crop is not None:
                start_pct, end_pct = crop
                y_top = page.rect.height * (start_pct / 100.0)
                y_bot = page.rect.height * (end_pct / 100.0)
                if multilang_y is not None:
                    y_bot = min(y_bot, multilang_y - 4)
                clip = fitz.Rect(0, y_top, right_x, y_bot)

            if clip is None:
                for kw in self._ui_keywords:
                    if re.search(re.escape(kw), page.get_text(), re.IGNORECASE):
                        hits = page.search_for(kw)
                        if hits:
                            y_top = max(0, hits[0].y0 - 8)
                            y_bot = multilang_y - 4 if multilang_y is not None else page.rect.height
                            clip = fitz.Rect(0, y_top, right_x, y_bot)
                            break

            if clip is None:
                y_top = page.rect.height * 0.4
                y_bot = multilang_y - 4 if multilang_y is not None else page.rect.height
                clip = fitz.Rect(0, y_top, right_x, y_bot)
                print("  [PDF] Using bottom-60% fallback crop")

            if clip.height <= 0:
                y_bot = multilang_y - 4 if multilang_y is not None else page.rect.height
                clip = fitz.Rect(0, 0, right_x, y_bot)
                print("  [PDF] Clip height was zero — reset to full-top crop")

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
            url = self._image_uploader(slug, png_bytes)
            print(f"  [PDF] UI diagram uploaded → {url}")
            return url
        except Exception as e:
            print(f"  [PDF] UI diagram upload failed: {e}")
            return None

    def _render_page_as_b64(self, page: fitz.Page, width: int = None, target_area: int = None) -> str:
        if target_area is not None:
            aspect = page.rect.width / page.rect.height
            width = int((target_area * aspect) ** 0.5)
        scale = width / page.rect.width
        mat = fitz.Matrix(scale, scale)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        return base64.b64encode(pix.tobytes("png")).decode()

    def _select_diagram_page_vision(self, doc: fitz.Document, candidate_indices: list[int]) -> int | None:
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
            raw = self._client.complete(
                system=(
                    "You are analyzing pages from a flashlight product manual. "
                    "Identify which page contains the UI/operation section."
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

    def _find_crop_vision(self, page: fitz.Page) -> tuple[float, float] | None:
        b64 = self._render_page_as_b64(page, width=_CROP_RENDER_WIDTH)
        try:
            raw = self._client.complete(
                system=(
                    "You are identifying the vertical location of a UI/operation diagram section "
                    "on a flashlight manual page."
                ),
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": b64}},
                        {"type": "text", "text": (
                            "At approximately what percentage range from the TOP of this image does the "
                            "UI/operation diagram section span? "
                            "Reply with ONLY two numbers separated by a dash, e.g. '40-100' or '0-28'."
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

    @staticmethod
    def _find_multilang_boundary_y(page: fitz.Page) -> float | None:
        lang_pattern = re.compile(
            r'\((DE|FR|ES|IT|JP|ZH|KO|PT|NL|PL|RU|AR|TR|CS|SV|DA|FI|NO|HU|RO|SK|UK)\)'
        )
        y_values = []
        for b in page.get_text("blocks"):
            text = b[4]
            if not lang_pattern.search(text):
                continue
            stripped = text.strip()
            if len(stripped) > 60:
                continue
            y_values.append(b[1])
        return min(y_values) if y_values else None

    @staticmethod
    def _find_content_column_x(page: fitz.Page) -> float | None:
        markers = ["CONTENTS", "Contents"]
        for b in page.get_text("blocks"):
            text = b[4].strip()
            if any(text.startswith(m) for m in markers):
                return b[0]
        return None
