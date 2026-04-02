# worker/galaxus.py
from __future__ import annotations

import asyncio
import base64
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote

import httpx
from playwright.async_api import async_playwright

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GALAXUS_SEARCH = "https://www.galaxus.ch/de/search?q={q}"
GALAXUS_BASE   = "https://www.galaxus.ch"

# Persisted query for product detail page (reverse-engineered from browser)
DETAIL_QUERY_ID  = "272a35585ebaa6d3ed984ff6584b75e5"
DETAIL_QUERY_URL = f"{GALAXUS_BASE}/graphql/o/{DETAIL_QUERY_ID}/productDetailPageQuery"

# Product offer query (works without cookies)
OFFER_QUERY_URL  = f"{GALAXUS_BASE}/api/graphql/get-products-with-offer-default"

# Portal ID for the X-Dg-Portal header (galaxus.ch = 22)
DG_PORTAL = "22"

# Availability classification → human-readable German text
CLASSIFICATION_TEXT: Dict[str, str] = {
    "SAME_DAY":            "Heute lieferbar",
    "ONE_DAY":             "Morgen lieferbar",
    "TWO_DAY":             "Übermorgen lieferbar",
    "THREE_TO_FIVE_DAYS":  "In 3–5 Tagen lieferbar",
    "SIX_TO_TEN_DAYS":     "In 6–10 Tagen lieferbar",
    "MORE_THAN_TEN_DAYS":  "Mehr als 10 Tage",
    "NOT_ORDERABLE":       "Nicht bestellbar",
    "NOT_AVAILABLE":       "Nicht verfügbar",
    "FUTURE_RELEASE":      "Noch nicht erschienen",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _sector_id_b64(sector_num: int) -> str:
    """Encode sector number in the base64 format Galaxus expects."""
    raw = f"Sector\ni{sector_num}"
    return base64.b64encode(raw.encode()).decode()


def _slug_from_url(url: str) -> str:
    """Return the path portion of a Galaxus product URL."""
    m = re.search(r"(\/de\/s\d+\/product\/[^?#]+)", url)
    return m.group(1) if m else ""


def _sector_from_slug(slug: str) -> int:
    m = re.search(r"/de/s(\d+)/product/", slug)
    return int(m.group(1)) if m else 6


def _numeric_product_id_from_slug(slug: str) -> Optional[int]:
    """Extract the trailing integer product ID from a slug like /de/s6/product/name-12345."""
    m = re.search(r"-(\d+)$", slug.rstrip("/"))
    return int(m.group(1)) if m else None


def _format_availability(availability: Dict) -> str:
    """
    Build a human-readable availability string from the API response.
    Example: "Morgen lieferbar – 2 Stück an Lager"
    """
    if not availability:
        return ""

    mail = availability.get("mail") or {}
    classification = mail.get("classification", "")
    text = CLASSIFICATION_TEXT.get(classification, classification)

    mail_detail = availability.get("mailDetail") or {}
    stock = mail_detail.get("stockDetails") or {}
    stock_count = stock.get("stockCount")
    status = stock.get("status", "")

    parts = [text] if text else []

    if status == "IN_STOCK" and stock_count is not None:
        parts.append(f"{stock_count} Stück an Lager")
    elif status == "OUT_OF_STOCK":
        parts.append("Nicht an Lager")
    elif status == "LOW_STOCK" and stock_count is not None:
        parts.append(f"Nur noch {stock_count} Stück")

    expected = mail_detail.get("expectedDelivery") or {}
    frm = expected.get("from") or ""
    to  = expected.get("to")  or ""
    if frm:
        try:
            dt_from = datetime.fromisoformat(frm.replace("Z", "+00:00"))
            date_str = dt_from.strftime("%d.%m.%Y")
            parts.append(f"Lieferung: {date_str}")
        except Exception:
            pass

    return " – ".join(parts)


# ---------------------------------------------------------------------------
# Step 1: resolve article number → product slug (Playwright)
# ---------------------------------------------------------------------------

async def _try_cookie_reject(page) -> None:
    for sel in [
        "button:has-text('Ablehnen')",
        "button:has-text('Reject')",
        "button:has-text('Nur notwendige akzeptieren')",
    ]:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(400)
                return
        except Exception:
            pass


async def _get_product_slug(product_id: str) -> str:
    """
    Uses a minimal Playwright session to load the Galaxus search results
    and return the slug of the first matching product.
    Returns "" on failure.
    """
    search_url = GALAXUS_SEARCH.format(q=quote(product_id, safe=""))
    slug = ""

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
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            await _try_cookie_reject(page)

            # If the search redirected straight to a product page
            if "/product/" in page.url:
                slug = _slug_from_url(page.url)
            else:
                # Wait for first product link to appear
                try:
                    await page.wait_for_selector(
                        "a[href*='/product/']", timeout=8000
                    )
                except Exception:
                    pass
                link = page.locator("a[href*='/product/']").first
                if await link.count() > 0:
                    href = await link.get_attribute("href") or ""
                    slug = _slug_from_url(href) or _slug_from_url(
                        GALAXUS_BASE + href if href.startswith("/") else href
                    )
        except Exception:
            pass
        finally:
            await context.close()
            await browser.close()

    return slug


# ---------------------------------------------------------------------------
# Step 2: fetch product detail via persisted GraphQL query (httpx, no cookies)
# ---------------------------------------------------------------------------

async def _fetch_product_detail(slug: str) -> Dict:
    """Call the productDetailPageQuery persisted query and return raw data dict."""
    sector_num = _sector_from_slug(slug)
    ninety_days_ago = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    variables = {
        "slug": slug,
        "offer": None,
        "shopArea": "RETAIL",
        "sectorId": _sector_id_b64(sector_num),
        "tagIds": [],
        "path": slug,
        "olderThan3MonthTimestamp": ninety_days_ago,
        "isSSR": False,
        "hasTrustLevelLowAndIsNotEProcurement": False,
        "adventCalendarEnabled": False,
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Dg-Portal": DG_PORTAL,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }
    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(
            DETAIL_QUERY_URL, headers=headers, json={"variables": variables}
        )
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Step 3: fetch description via offer query (httpx, no cookies)
# ---------------------------------------------------------------------------

async def _fetch_description(product_numeric_id: int) -> str:
    """Return the product description text from the offer query."""
    query = """
    query {
      productsWithOfferDefault(productIds: [%d]) {
        products {
          product { description }
        }
      }
    }
    """ % product_numeric_id

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                OFFER_QUERY_URL, headers=headers, json={"query": query}
            )
            resp.raise_for_status()
            data = resp.json()
            products = (
                data.get("data", {})
                .get("productsWithOfferDefault", {})
                .get("products", [])
            )
            if products:
                return (products[0].get("product") or {}).get("description") or ""
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# Main public function
# ---------------------------------------------------------------------------

async def check_galaxus_product(product_id: str) -> Dict[str, Any]:
    """
    Full product check.  Returns:
      url             : str
      availability_text: str   (formatted German text)
      images_count    : int
      videos_count    : int
      bullets_ok      : bool   (description > 100 chars)
      price_chf       : str
      notes           : str    (debug / error info)
    """
    notes = ""

    # --- Step 1: resolve slug via Playwright search ---
    slug = await _get_product_slug(product_id)
    if not slug:
        return {
            "url": "",
            "availability_text": "",
            "images_count": 0,
            "videos_count": 0,
            "bullets_ok": False,
            "price_chf": "",
            "notes": f"Produkt nicht gefunden für '{product_id}'",
        }

    url = GALAXUS_BASE + slug
    product_numeric_id = _numeric_product_id_from_slug(slug)

    # --- Step 2 + 3: fetch detail and description concurrently ---
    async def _empty_desc() -> str:
        return ""

    detail_task = asyncio.create_task(_fetch_product_detail(slug))
    desc_task   = asyncio.create_task(
        _fetch_description(product_numeric_id)
        if product_numeric_id
        else _empty_desc()
    )

    try:
        detail_data = await detail_task
    except Exception as e:
        notes = f"Detail API Fehler: {e}"
        detail_data = {}

    try:
        description = await desc_task
    except Exception as e:
        description = ""
        notes += f" | Desc API Fehler: {e}"

    # --- Parse detail response ---
    product = (detail_data.get("data") or {}).get("product") or {}

    gallery_count = (product.get("galleryImages") or {}).get("totalCount", 0)
    video_count   = (product.get("videos") or {}).get("totalCount", 0)

    availability     = product.get("availability") or {}
    availability_txt = _format_availability(availability)

    price_obj = product.get("price") or {}
    price_chf = str(price_obj.get("amountInclusive", "")) if price_obj else ""

    bullets_ok = len(_clean(description)) > 100

    return {
        "url":               url,
        "availability_text": availability_txt,
        "images_count":      gallery_count,
        "videos_count":      video_count,
        "bullets_ok":        bullets_ok,
        "price_chf":         price_chf,
        "notes":             notes,
    }


# ---------------------------------------------------------------------------
# Keyword ranking (unchanged logic, kept for Excel compatibility)
# ---------------------------------------------------------------------------

async def check_keyword_rank(keyword: str, product_id: str) -> Dict[str, Any]:
    """
    Checks keyword ranking up to 48 scroll positions.
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

        await page.goto(
            GALAXUS_SEARCH.format(q=keyword),
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await _try_cookie_reject(page)
        await page.wait_for_timeout(800)

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
