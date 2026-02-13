# app/worker/galaxus.py
from __future__ import annotations
import re
from typing import Dict, Any
from robocorp import browser

GALAXUS_SEARCH = "https://www.galaxus.ch/de/search?q={q}"

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _price_to_chf(text: str) -> str:
    m = re.search(r"CHF\s*([\d'.,]+)", text or "")
    return m.group(1).replace("'", "") if m else ""

def _extract_availability_text(page) -> str:
    """
    Strategy:
    1) Locate the small icon area under the main product image (right side).
    2) Try attributes first (aria-label/title/data-tooltip).
    3) Fallback: visible tooltip after hover.
    """
    # TODO: replace with a stable selector after inspection!
    # Start with a "search by candidates" approach:
    candidates = page.locator("[aria-label*='geliefert'], [aria-label*='Stück'], [title*='geliefert'], [title*='Stück']")
    if candidates.count() > 0:
        # pick first match, read attributes/text
        el = candidates.first
        for attr in ["aria-label", "title", "data-tooltip", "data-original-title"]:
            try:
                v = el.get_attribute(attr)
                if v and _clean(v):
                    return _clean(v)
            except Exception:
                pass
        try:
            t = el.inner_text()
            if _clean(t):
                return _clean(t)
        except Exception:
            pass

    # Hover-based fallback: hover over the icon area, read tooltip container
    # TODO: replace tooltip selector after inspection!
    try:
        icon_area = page.locator("css=section")  # placeholder; refine
        icon_area.hover()
        page.wait_for_timeout(500)
        tooltip = page.locator("[role='tooltip']").first
        if tooltip.count() > 0:
            return _clean(tooltip.inner_text())
    except Exception:
        pass

    return ""  # if nothing found

def check_galaxus_product(product_id: str) -> Dict[str, Any]:
    page = browser.page()
    page.goto(GALAXUS_SEARCH.format(q=product_id))
    page.wait_for_load_state()

    # cookie popup handling (pattern from your tasks.py)
    for sel in ["[name=reject]", "button:has-text('Ablehnen')", "button:has-text('Reject')"]:
        try:
            btn = page.locator(sel)
            if btn.count():
                btn.first.click()
                break
        except Exception:
            pass

    page.wait_for_timeout(1000)

    # click a search result that contains product_id
    # TODO refine selector after inspection
    link = page.locator(f"a:has-text('{product_id}')").first
    if link.count() == 0:
        raise RuntimeError(f"Product ID not found in search results: {product_id}")
    link.click()
    page.wait_for_load_state()
    page.wait_for_timeout(1000)

    availability_text = _extract_availability_text(page)

    # counts: refine selectors later (gallery + videos)
    images_count = page.locator("img").count()
    videos_count = page.locator("video").count()

    # bullets: operational definition (>=5 list items with short text)
    lis = page.locator("li")
    short_li = 0
    for i in range(min(lis.count(), 200)):
        txt = _clean(lis.nth(i).inner_text())
        if 0 < len(txt) <= 220:
            short_li += 1
    bullets_ok = short_li >= 5

    price_text = ""
    price_candidates = page.locator("text=/CHF\\s*[\\d'.,]+/")
    if price_candidates.count() > 0:
        price_text = price_candidates.first.inner_text()
    price_chf = _price_to_chf(price_text)

    return {
        "url": page.url,
        "availability_text": availability_text,
        "images_count": images_count,
        "videos_count": videos_count,
        "bullets_ok": bullets_ok,
        "price_chf": price_chf,
    }