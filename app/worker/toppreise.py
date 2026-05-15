# worker/toppreise.py
"""
Toppreise check: best price among allowed vendors.

Returns { "best_price_chf": float|"" , "vendor": str }
- best_price_chf is a plain number (no "CHF")
- vendor is a human-readable name (e.g. "Fust")
"""
from __future__ import annotations

import re
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import quote

from playwright.async_api import async_playwright, Page

TOPPREISE_SEARCH = "https://www.toppreise.ch/search?q={q}"

# Erlaubte Anbieter mit kanonischem Anzeigenamen.
# Pattern matching ist case-insensitive; key = Substring im Container-Text,
# value = wie der Anbieter in Excel geschrieben werden soll.
ALLOWED_VENDORS: List[Tuple[str, str]] = [
    ("interdiscount", "Interdiscount"),
    ("media markt",   "MediaMarkt"),
    ("mediamarkt",    "MediaMarkt"),
    ("nettoshop",     "Nettoshop"),
    ("brack",         "Brack"),
    ("fust",          "Fust"),
    ("philips",       "Philips"),
    ("baby-markt",    "Babymarkt"),
    ("babymarkt",     "Babymarkt"),
    ("baby-walz",     "Babywalz"),
    ("babywalz",      "Babywalz"),
]


def _clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _parse_price(text: str) -> Optional[float]:
    """Parse CHF price like 'CHF 249.95', "1'249.00", '249,95' -> float."""
    if not text:
        return None
    m = re.search(r"([\d'.,]+)", text)
    if not m:
        return None
    s = m.group(1).replace("'", "")
    # Falls Komma als Dezimaltrenner: ersetze letztes Komma durch Punkt
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _vendor_from_text(container_text: str) -> str:
    if not container_text:
        return ""
    t = container_text.lower()
    for pattern, canonical in ALLOWED_VENDORS:
        if pattern in t:
            return canonical
    return ""


async def _try_cookie_accept(page: Page) -> None:
    for sel in (
        "button:has-text('OK')",
        "button:has-text('Akzeptieren')",
        "button:has-text('Accept')",
        "button:has-text('Einverstanden')",
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
    Sucht den günstigsten erlaubten Anbieter auf Toppreise.

    Vorgehen:
      1. /search?q=<product_id> aufrufen
      2. Ersten plausiblen Produkt-Treffer öffnen
      3. Alle Angebote scannen: Preis + Container-Text
      4. Nur Angebote von erlaubten Anbietern behalten
      5. Günstigsten zurückgeben
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="de-CH",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = await context.new_page()

        try:
            await page.goto(
                TOPPREISE_SEARCH.format(q=quote(product_id, safe="")),
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await _try_cookie_accept(page)
            await page.wait_for_timeout(900)

            # Auf das erste plausible Produkt-Result klicken
            result_link = page.locator(
                "a[href*='/price/'], a[href*='/produkte/'], a[href*='/product/']"
            ).first
            if await result_link.count() > 0:
                try:
                    await result_link.click(timeout=8000)
                    await page.wait_for_load_state("domcontentloaded")
                    await page.wait_for_timeout(900)
                except Exception:
                    pass

            offers: List[Tuple[str, float]] = []

            # Alle CHF-Preis-Knoten finden
            price_nodes = page.locator("text=/CHF\\s*[\\d'.,]+/")
            n = await price_nodes.count()

            for i in range(min(n, 120)):
                node = price_nodes.nth(i)
                price_text = _clean(await node.text_content())
                price = _parse_price(price_text)
                if price is None or price <= 0:
                    continue

                # Vendor aus nächstem sinnvollen Container ableiten
                vendor = ""
                try:
                    container = node.locator(
                        "xpath=ancestor::*[self::tr or self::li or self::div][1]"
                    )
                    container_text = _clean(await container.inner_text())
                    vendor = _vendor_from_text(container_text)
                except Exception:
                    pass

                if vendor:
                    offers.append((vendor, price))

            if not offers:
                return {"best_price_chf": "", "vendor": ""}

            # Günstigsten finden
            vendor, best = min(offers, key=lambda x: x[1])
            return {"best_price_chf": best, "vendor": vendor}
        finally:
            await context.close()
            await browser.close()
