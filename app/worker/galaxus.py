# worker/galaxus.py
from __future__ import annotations
import re
from typing import Dict, Any, Optional, List
from playwright.async_api import async_playwright, Page

GALAXUS_SEARCH = "https://www.galaxus.ch/de/search?q={q}"

AVAILABILITY_HINTS = [
    "geliefert", "stück", "lager", "lieferant", "morgen", "zwischen"
]

def _clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _price_to_chf(text: str) -> str:
    # Extract "CHF 199.–" or "CHF 199.00" -> "199" / "199.00"
    m = re.search(r"CHF\s*([\d'.,]+)", text or "")
    return m.group(1).replace("'", "") if m else ""

async def _try_cookie_reject(page: Page) -> None:
    selectors = [
        "button:has-text('Ablehnen')",
        "button:has-text('Reject')",
        "button:has-text('Nur notwendige akzeptieren')",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(400)
                return
        except Exception:
            pass

async def _get_attr_if_any(el, attrs: List[str]) -> str:
    for a in attrs:
        try:
            v = await el.get_attribute(a)
            if _clean(v):
                return _clean(v)
        except Exception:
            pass
    return ""

async def extract_availability_text(page: Page) -> str:
    """
    Goal: extract the hidden tooltip/label text behind the small icon under the main product image.
    Typical text includes e.g.:
      - "morgen geliefert"
      - "Nur 3 Stück an Lager / Zwischen ... geliefert"
      - "Mehr als 10 Stück an Lager beim Lieferanten / ..."
    Strategy (robust, selector-light):
      1) Search for elements with aria-label/title/data-* containing typical words.
      2) If nothing found: hover likely icon candidates and read tooltip role='tooltip'.
    """

    # 1) Attribute-based search across the page (then we can narrow later if needed)
    attribute_candidates = page.locator(
        "[aria-label], [title], [data-tooltip], [data-original-title]"
    )
    n = await attribute_candidates.count()
    for i in range(min(n, 2500)):  # cap to avoid expensive scans
        el = attribute_candidates.nth(i)
        s = ""
        s = await _get_attr_if_any(el, ["aria-label", "title", "data-tooltip", "data-original-title"])
        low = s.lower()
        if s and any(h in low for h in AVAILABILITY_HINTS):
            # Often availability strings are exactly what we want
            return s

    # 2) Hover-based fallback: try hovering common icon containers
    hover_selectors = [
        # Common tooltip triggers
        "svg[aria-label]", "svg[title]",
        "[data-tooltip]", "[title]",
        # Sometimes availability icon is in a small 'info' cluster
        "button[aria-label]", "span[aria-label]",
    ]

    for sel in hover_selectors:
        try:
            cand = page.locator(sel)
            if await cand.count() == 0:
                continue
            el = cand.first
            await el.hover(timeout=1500)
            await page.wait_for_timeout(300)

            tooltip = page.locator("[role='tooltip']").first
            if await tooltip.count() > 0:
                t = _clean(await tooltip.inner_text())
                if t and any(h in t.lower() for h in AVAILABILITY_HINTS):
                    return t
        except Exception:
            pass

    return ""

async def check_galaxus_product(product_id: str) -> Dict[str, Any]:
    """
    Returns:
      availability_text: str
      images_count: int
      videos_count: int
      bullets_ok: bool
      price_chf: str
      url: str
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        )
        page = await context.new_page()

        await page.goto(GALAXUS_SEARCH.format(q=product_id), wait_until="domcontentloaded", timeout=60000)
        await _try_cookie_reject(page)
        await page.wait_for_timeout(800)

        # Click result containing product_id (best-effort)
        link = page.locator(f"a:has-text('{product_id}')").first
        if await link.count() == 0:
            # fallback: click first plausible product link card
            # (this is a fallback; ideally refine after first live run)
            link = page.locator("a[href*='/']").first

        await link.click(timeout=10000)
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(900)

        availability_text = await extract_availability_text(page)

        # --- Media counts (refine selectors later if needed) ---
        # Prefer counting within a product gallery container if detectable.
        # Generic fallback:
        images_count = await page.locator("img").count()
        videos_count = await page.locator("video").count()

        # --- Bullet check ---
        # Operational: count short <li> items (<=220 chars) and require >=5.
        lis = page.locator("li")
        li_n = await lis.count()
        short_li = 0
        for i in range(min(li_n, 250)):
            txt = _clean(await lis.nth(i).inner_text())
            if 0 < len(txt) <= 220:
                short_li += 1
        bullets_ok = short_li >= 5

        # --- Price ---
        price_text = ""
        price_candidates = page.locator("text=/CHF\\s*[\\d'.,]+/")
        if await price_candidates.count() > 0:
            price_text = _clean(await price_candidates.first.text_content())
        price_chf = _price_to_chf(price_text)

        url = page.url

        await context.close()
        await browser.close()

        return {
            "url": url,
            "availability_text": availability_text,
            "images_count": images_count,
            "videos_count": videos_count,
            "bullets_ok": bullets_ok,
            "price_chf": price_chf,
        }

async def check_keyword_rank(keyword: str, product_id: str) -> Dict[str, Any]:
    """
    Checks keyword ranking up to 48 items.
    Because the Excel column for ranking was removed, we store this in JSON only.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()

        await page.goto(GALAXUS_SEARCH.format(q=keyword), wait_until="domcontentloaded", timeout=60000)
        await _try_cookie_reject(page)
        await page.wait_for_timeout(800)

        # Best-effort scanning:
        # Look for product_id text presence as we scroll.
        for rank in range(1, 49):
            html = await page.content()
            if product_id in html:
                await context.close()
                await browser.close()
                return {"rank": rank, "over_48": False}

            await page.mouse.wheel(0, 1200)
            await page.wait_for_timeout(350)

        await context.close()
        await browser.close()
        return {"rank": "", "over_48": True}
