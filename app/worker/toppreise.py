# worker/toppreise.py
from __future__ import annotations

import re
from typing import Dict, Any, Optional, Tuple, List

from playwright.async_api import async_playwright, Page

TOPPREISE_SEARCH = "https://www.toppreise.ch/search?q={q}"

# Case-insensitive vendor pattern matching (includes hyphen + domain variants)
ALLOWED_VENDOR_PATTERNS: List[str] = [
    "interdiscount",
    "mediamarkt",
    "nettoshop",
    "brack",
    "fust",
    "philips",
    # baby vendors (all relevant forms)
    "babymarkt",
    "baby-markt",
    "babymarkt.com",
    "babymarkt.ch",
    "babywalz",
    "baby-walz",
    "baby-walz.ch",
]

def _clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"([\d'.,]+)", text)
    if not m:
        return None
    s = m.group(1).replace("'", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def _normalize_vendor_from_text(container_text: str) -> str:
    """
    Detect allowed vendors in a blob of text (case-insensitive),
    supporting hyphen and domain variants, and return a canonical vendor name.
    Returns "" if no allowed vendor is detected.
    """
    t = (container_text or "").lower()

    for pattern in ALLOWED_VENDOR_PATTERNS:
        if pattern in t:
            # Canonicalize baby vendors
            if "babymarkt" in pattern or "baby-markt" in pattern:
                return "Babymarkt"
            if "babywalz" in pattern or "baby-walz" in pattern:
                return "Babywalz"

            # Canonicalize others (simple title-case)
            # Note: Mediamarkt -> Mediamarkt (acceptable), Interdiscount -> Interdiscount, etc.
            return pattern.capitalize()

    return ""

async def _try_cookie_accept(page: Page) -> None:
    for sel in (
        "button:has-text('OK')",
        "button:has-text('Akzeptieren')",
        "button:has-text('Accept')",
    ):
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

    Output:
      - best_price_chf: float or ""
      - vendor: str or ""

    If no allowed vendor offer found: returns empty strings.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()

        await page.goto(
            TOPPREISE_SEARCH.format(q=product_id),
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await _try_cookie_accept(page)
        await page.wait_for_timeout(900)

        # Click first plausible result (best-effort; refine with stable selector after first live run)
        result_link = page.locator(
            "a[href*='/price/'], a[href*='/produkte/'], a[href*='/product/']"
        ).first

        if await result_link.count() > 0:
            await result_link.click(timeout=8000)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(800)

        offers: List[Tuple[str, float]] = []

        # Heuristic: scan for CHF prices, then look for allowed vendor in a nearby container
        price_nodes = page.locator("text=/CHF\\s*[\\d'.,]+/")
        pn = await price_nodes.count()

        for i in range(min(pn, 80)):
            node = price_nodes.nth(i)
            price_text = _clean(await node.text_content())
            price = _parse_price(price_text)
            if price is None:
                continue

            vendor = ""
            try:
                container = node.locator(
                    "xpath=ancestor::*[self::tr or self::li or self::div][1]"
                )
                container_text = _clean(await container.inner_text())
                vendor = _normalize_vendor_from_text(container_text)
            except Exception:
                vendor = ""

            if vendor:
                offers.append((vendor, price))

        best_vendor = ""
        best_price: Optional[float] = None

        for vendor, price in offers:
            if best_price is None or price < best_price:
                best_price = price
                best_vendor = vendor

        await context.close()
        await browser.close()

        if best_price is None:
            return {"best_price_chf": "", "vendor": ""}

        return {"best_price_chf": best_price, "vendor": best_vendor}
