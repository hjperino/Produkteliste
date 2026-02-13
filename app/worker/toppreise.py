# worker/toppreise.py
from __future__ import annotations
import re
from typing import Dict, Any, Optional, Tuple, List
from playwright.async_api import async_playwright, Page

TOPPREISE_SEARCH = "https://www.toppreise.ch/search?q={q}"

ALLOWED_VENDORS = {"Interdiscount", "MediaMarkt", "Nettoshop", "Brack", "Fust", "Philips"}

def _clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    # Find first number-like thing
    m = re.search(r"([\d'.,]+)", text)
    if not m:
        return None
    s = m.group(1).replace("'", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

async def _try_cookie_accept(page: Page) -> None:
    # Keep it minimal; extend after first test if needed
    for sel in ["button:has-text('OK')", "button:has-text('Akzeptieren')", "button:has-text('Accept')"]:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass

async def check_toppreise(product_id: str) -> Dict[str, Any]:
    """
    Returns best allowed vendor price from Toppreise.
    If none found: empty strings.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()

        await page.goto(TOPPREISE_SEARCH.format(q=product_id), wait_until="domcontentloaded", timeout=60000)
        await _try_cookie_accept(page)
        await page.wait_for_timeout(900)

        # Click first search result (best-effort).
        # You may refine selector after first run.
        result_link = page.locator("a[href*='/price/'], a[href*='/produkte/'], a[href*='/product/']").first
        if await result_link.count() > 0:
            await result_link.click(timeout=8000)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(800)

        # Try to discover offers by scanning for CHF prices and vendor text nearby.
        # This is heuristic; tighten later using stable DOM selectors.
        offers: List[Tuple[str, float]] = []

        price_nodes = page.locator("text=/CHF\\s*[\\d'.,]+/")
        pn = await price_nodes.count()
        for i in range(min(pn, 80)):
            node = price_nodes.nth(i)
            price_text = _clean(await node.text_content())
            price = _parse_price(price_text)
            if price is None:
                continue

            # attempt to find vendor in ancestor container
            vendor = ""
            try:
                container = node.locator("xpath=ancestor::*[self::tr or self::li or self::div][1]")
                t = _clean(await container.inner_text())
                # vendor match by allowed names
                for v in ALLOWED_VENDORS:
                    if v.lower() in t.lower():
                        vendor = v
                        break
            except Exception:
                vendor = ""

            if vendor and price is not None:
                offers.append((vendor, price))

        best_vendor = ""
        best_price: Optional[float] = None
        for vendor, price in offers:
            if vendor in ALLOWED_VENDORS:
                if best_price is None or price < best_price:
                    best_price = price
                    best_vendor = vendor

        await context.close()
        await browser.close()

        if best_price is None:
            return {"best_price_chf": "", "vendor": ""}

        return {"best_price_chf": best_price, "vendor": best_vendor}
