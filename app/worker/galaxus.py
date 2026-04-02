# worker/galaxus.py
from __future__ import annotations
import re
from urllib.parse import quote
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
    Galaxus shows availability as innerText of span[role="button"][aria-haspopup="dialog"].
    There are multiple such spans on the page (icons etc.) — we pick the one
    whose text contains known availability keywords.
    Example: "Übermorgen geliefert\nNur 2 Stück an Lager"
    """
    spans = page.locator('span[role="button"][aria-haspopup="dialog"]')
    n = await spans.count()
    for i in range(n):
        t = _clean(await spans.nth(i).inner_text())
        if t and any(h in t.lower() for h in AVAILABILITY_HINTS):
            return t
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
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-http2",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = await context.new_page()

        await page.goto(GALAXUS_SEARCH.format(q=quote(product_id, safe="")), wait_until="domcontentloaded", timeout=60000)
        await _try_cookie_reject(page)
        await page.wait_for_timeout(1500)

        # If search redirected directly to a product page, skip click
        if "/product/" not in page.url:
            # Wait for product links to render (Galaxus is a SPA)
            try:
                await page.wait_for_selector("a[href*='/product/']", timeout=6000)
            except Exception:
                pass

            # Click the first actual product page link (not navigation/logo)
            link = page.locator("a[href*='/product/']").first
            if await link.count() > 0:
                await link.click(timeout=10000)
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(900)

        # Wait for SPA content to fully render
        try:
            await page.wait_for_selector('span[role="button"][aria-haspopup="dialog"]', timeout=5000)
        except Exception:
            pass
        await page.wait_for_timeout(500)

        availability_text = await extract_availability_text(page)

        # --- Images and videos: Galaxus shows "25 Bilder" / "1 Video" as button text ---
        images_count = 0
        videos_count = 0
        btns = page.locator("button")
        btn_n = await btns.count()
        for i in range(min(btn_n, 60)):
            btn_text = _clean(await btns.nth(i).inner_text())
            m_img = re.search(r'(\d+)\s*Bilder?', btn_text)
            if m_img:
                images_count = int(m_img.group(1))
            m_vid = re.search(r'(\d+)\s*Videos?', btn_text)
            if m_vid:
                videos_count = int(m_vid.group(1))

        # --- Product description check (Beschreibung section > 100 chars) ---
        bullets_ok = await page.evaluate("""() => {
            const h = [...document.querySelectorAll('h2,h3')].find(el => el.innerText.trim() === 'Beschreibung');
            if (!h) return false;
            const next = h.nextElementSibling;
            return next ? next.innerText.trim().length > 100 : false;
        }""")

        # --- Price: first <strong> containing CHF ---
        price_chf = ""
        strongs = page.locator("strong")
        strong_n = await strongs.count()
        for i in range(min(strong_n, 20)):
            t = _clean(await strongs.nth(i).inner_text())
            if "CHF" in t:
                m = re.search(r"([\d',.]+)", t.replace("CHF", "").strip())
                if m:
                    price_chf = m.group(1).replace("'", "")
                    break

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
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
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
