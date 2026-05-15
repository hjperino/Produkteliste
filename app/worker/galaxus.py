# worker/galaxus.py
"""
Galaxus product check via persisted GraphQL queries.

Public functions
----------------
check_galaxus_product(product_id) -> dict
    Returns availability ja/nein, image/video counts, description-OK flag,
    galaxus price (float), main image URL, and Galaxus URL.

check_keyword_rank(keyword, product_id) -> dict
    Returns the position (1..48) of the product on Galaxus when searching
    for `keyword`. Returns "" if not found within 48 results.
"""
from __future__ import annotations

import asyncio
import base64
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx
from playwright.async_api import async_playwright

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
GALAXUS_BASE      = "https://www.galaxus.ch"
GALAXUS_SEARCH    = f"{GALAXUS_BASE}/de/search?q={{q}}"

SEARCH_QUERY_ID   = "1550dffd10716adbe05c612c391fac32"
SEARCH_QUERY_URL  = f"{GALAXUS_BASE}/graphql/o/{SEARCH_QUERY_ID}/useSearchDataQuery"

DETAIL_QUERY_ID   = "272a35585ebaa6d3ed984ff6584b75e5"
DETAIL_QUERY_URL  = f"{GALAXUS_BASE}/graphql/o/{DETAIL_QUERY_ID}/productDetailPageQuery"

OFFER_QUERY_URL   = f"{GALAXUS_BASE}/api/graphql/get-products-with-offer-default"

DG_PORTAL = "22"  # galaxus.ch

_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-Dg-Portal": DG_PORTAL,
    "Accept-Language": "de-CH,de;q=0.9,en;q=0.7",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _strip_html(html: str) -> str:
    """Remove HTML tags but keep paragraph/li breaks as newlines."""
    if not html:
        return ""
    s = re.sub(r"(?i)</(p|li|div|br)\s*/?>", "\n", html)
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"\r", "", s)
    return s


def _sector_id_b64(sector_num: int) -> str:
    raw = f"Sector\ni{sector_num}"
    return base64.b64encode(raw.encode()).decode()


def _slug_de(relative_url: str) -> str:
    return re.sub(r"^/en/", "/de/", relative_url or "")


def _sector_from_slug(slug: str) -> int:
    m = re.search(r"/(?:de|en)/s(\d+)/product/", slug)
    return int(m.group(1)) if m else 6


def _numeric_id_from_slug(slug: str) -> Optional[int]:
    m = re.search(r"-(\d+)$", (slug or "").rstrip("/"))
    return int(m.group(1)) if m else None


def _availability_ja_nein(detail_product: Dict) -> str:
    """
    Hans-Regel:
      "X Stück am Lager" -> ja
      "nicht am Lager"   -> nein
    """
    avail = (detail_product.get("availability") or {})
    mail_detail = avail.get("mailDetail") or {}
    stock = mail_detail.get("stockDetails") or {}
    status = stock.get("status", "")
    stock_count = stock.get("stockCount")

    if status in ("IN_STOCK", "LOW_STOCK") and stock_count is not None and stock_count > 0:
        return "ja"

    # Klassifikation als Fallback
    classification = (avail.get("mail") or {}).get("classification", "")
    if classification in ("SAME_DAY", "ONE_DAY", "TWO_DAY", "THREE_TO_FIVE_DAYS"):
        return "ja"

    return "nein"


def _description_ok(description_html: str) -> bool:
    """
    Hans-Regel: längerer Einführungstext + mindestens 5 kurze Bullets/Absätze.
    Wir parsen den HTML-Block in Absätze (anhand <p>, <li>, <br>, \\n) und prüfen:
      - mindestens ein Absatz > 300 Zeichen
      - mindestens 5 weitere Absätze (Bullets) mit je 20-400 Zeichen
    """
    text = _strip_html(description_html)
    parts = [p.strip() for p in re.split(r"\n+", text) if p.strip()]
    long_blocks = [p for p in parts if len(p) >= 300]
    short_blocks = [p for p in parts if 20 <= len(p) <= 400]
    return bool(long_blocks) and len(short_blocks) >= 5


def _main_image_url(product: Dict) -> str:
    gallery = (product.get("galleryImages") or {}).get("edges") or []
    if gallery:
        node = gallery[0].get("node") or {}
        for key in ("largeUrl", "url", "originalUrl", "highResUrl"):
            if node.get(key):
                return node[key]
    main = product.get("mainImage") or {}
    return main.get("url") or main.get("largeUrl") or ""


# ---------------------------------------------------------------------------
# Step 1: search by article number
# ---------------------------------------------------------------------------
async def _search_product(query: str, first: int = 1) -> List[Dict]:
    variables = {
        "query": query,
        "searchQueryConfig": {},
        "first": first,
        "sortOrder": "RELEVANCE",
    }
    async with httpx.AsyncClient(timeout=20.0, headers=_HEADERS) as client:
        resp = await client.post(SEARCH_QUERY_URL, json={"variables": variables})
        resp.raise_for_status()
        data = resp.json()

    edges = (
        (data.get("data") or {})
        .get("shopSearch", {})
        .get("products", {})
        .get("edges", [])
    )
    return [e.get("node") or {} for e in edges]


# ---------------------------------------------------------------------------
# Step 2: product detail
# ---------------------------------------------------------------------------
async def _fetch_product_detail(slug: str) -> Dict:
    sector = _sector_from_slug(slug)
    ninety_days_ago = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    variables = {
        "slug": slug,
        "offer": None,
        "shopArea": "RETAIL",
        "sectorId": _sector_id_b64(sector),
        "tagIds": [],
        "path": slug,
        "olderThan3MonthTimestamp": ninety_days_ago,
        "isSSR": False,
        "hasTrustLevelLowAndIsNotEProcurement": False,
        "adventCalendarEnabled": False,
    }
    async with httpx.AsyncClient(timeout=20.0, headers=_HEADERS) as client:
        resp = await client.post(DETAIL_QUERY_URL, json={"variables": variables})
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Step 3: description via offer query
# ---------------------------------------------------------------------------
async def _fetch_description(product_numeric_id: int) -> str:
    query = (
        "query {"
        f" productsWithOfferDefault(productIds: [{product_numeric_id}]) {{"
        "   products { product { description } }"
        " }"
        "}"
    )
    try:
        async with httpx.AsyncClient(timeout=15.0, headers=_HEADERS) as client:
            resp = await client.post(OFFER_QUERY_URL, json={"query": query})
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
# Public: full product check
# ---------------------------------------------------------------------------
async def check_galaxus_product(product_id: str) -> Dict[str, Any]:
    """
    Returns dict:
      url                : str       Galaxus product URL
      available           : "ja"/"nein"
      images_ok           : "ja"/"nein"   (>= 6 Bilder)
      videos_ok           : "ja"/"nein"   (>= 1 Video)
      bullets_ok          : "ja"/"nein"   (Langtext + >=5 Bullets)
      price_chf           : float | ""    reine Zahl
      main_image_url      : str
      notes               : str           Debug info
    """
    notes = ""

    # Suche: zuerst mit Original-ID, dann ohne "/00" falls leer
    try:
        nodes = await _search_product(product_id)
    except Exception as e:
        nodes = []
        notes = f"Search API: {e}"

    if not nodes:
        stripped = re.sub(r"/\d+$", "", product_id)
        if stripped and stripped != product_id:
            try:
                nodes = await _search_product(stripped)
                if nodes:
                    notes = f"gefunden mit Fallback-Query '{stripped}'"
            except Exception as e:
                notes += f" | Fallback search: {e}"

    if not nodes:
        return {
            "url": "",
            "available": "nein",
            "images_ok": "nein",
            "videos_ok": "nein",
            "bullets_ok": "nein",
            "price_chf": "",
            "main_image_url": "",
            "notes": notes or f"Produkt nicht gefunden: '{product_id}'",
        }

    node = nodes[0]
    slug = _slug_de(node.get("relativeUrl", ""))
    url = GALAXUS_BASE + slug if slug else ""
    product_numeric_id = _numeric_id_from_slug(slug)

    # Detail + Description parallel
    async def _empty() -> str:
        return ""

    detail_task = asyncio.create_task(_fetch_product_detail(slug))
    desc_task = asyncio.create_task(
        _fetch_description(product_numeric_id) if product_numeric_id else _empty()
    )

    try:
        detail_data = await detail_task
    except Exception as e:
        detail_data = {}
        notes += f" | Detail API: {e}"

    try:
        description = await desc_task
    except Exception as e:
        description = ""
        notes += f" | Desc API: {e}"

    product = (detail_data.get("data") or {}).get("product") or {}

    gallery_count = (product.get("galleryImages") or {}).get("totalCount", 0)
    video_count = (product.get("videos") or {}).get("totalCount", 0)

    available = _availability_ja_nein(product) if product else "nein"

    # Preis als reine Zahl
    price_obj = product.get("price") or {}
    price_val = price_obj.get("amountInclusive")
    if price_val is None:
        # Fallback aus Search-Result
        price_val = (node.get("price") or {}).get("amountInclusive")
    try:
        price_chf = float(price_val) if price_val is not None and price_val != "" else ""
    except (TypeError, ValueError):
        price_chf = ""

    return {
        "url": url,
        "available": available,
        "images_ok": "ja" if gallery_count >= 6 else "nein",
        "videos_ok": "ja" if video_count >= 1 else "nein",
        "bullets_ok": "ja" if _description_ok(description) else "nein",
        "price_chf": price_chf,
        "main_image_url": _main_image_url(product),
        "notes": notes.strip(" |"),
    }


# ---------------------------------------------------------------------------
# Public: keyword ranking (Playwright)
# ---------------------------------------------------------------------------
async def _try_cookie_reject(page) -> None:
    for sel in (
        "button:has-text('Ablehnen')",
        "button:has-text('Reject')",
        "button:has-text('Nur notwendige akzeptieren')",
    ):
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(400)
                return
        except Exception:
            pass


async def check_keyword_rank(keyword: str, product_id: str) -> Dict[str, Any]:
    """
    Returns {"rank": int|"", "over_48": bool}.
    Scrolls the search-result page until the product_id appears in the DOM.
    """
    if not keyword or not product_id:
        return {"rank": "", "over_48": False}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=_HEADERS["User-Agent"],
            locale="de-CH",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = await context.new_page()

        await page.goto(
            GALAXUS_SEARCH.format(q=quote(keyword, safe="")),
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await _try_cookie_reject(page)
        await page.wait_for_timeout(800)

        try:
            for rank in range(1, 49):
                html = await page.content()
                if product_id in html:
                    return {"rank": rank, "over_48": False}
                await page.mouse.wheel(0, 1200)
                await page.wait_for_timeout(350)
            return {"rank": "", "over_48": True}
        finally:
            await context.close()
            await browser.close()
