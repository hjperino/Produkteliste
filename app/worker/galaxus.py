# worker/galaxus.py
"""
Galaxus product check via persisted GraphQL queries with detailed logging.

Public functions
----------------
check_galaxus_product(product_id) -> dict
    Returns availability ja/nein, image/video counts, description-OK flag,
    galaxus price (float), main image URL, Galaxus URL, plus a 'log' string
    that describes every step (visible in the Excel 'Debug' column).

check_keyword_rank(keyword, product_id) -> dict
    Position (1..48) of the product on Galaxus when searching for `keyword`.
"""
from __future__ import annotations

import asyncio
import base64
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
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
    "Accept-Language": "de-CH,de;q=0.9,en;q=0.7",
    "Origin": GALAXUS_BASE,
    "Referer": f"{GALAXUS_BASE}/de",
    "X-Dg-Portal": DG_PORTAL,
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
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
    avail = (detail_product.get("availability") or {})
    mail_detail = avail.get("mailDetail") or {}
    stock = mail_detail.get("stockDetails") or {}
    status = stock.get("status", "")
    stock_count = stock.get("stockCount")

    if status in ("IN_STOCK", "LOW_STOCK") and stock_count is not None and stock_count > 0:
        return "ja"

    classification = (avail.get("mail") or {}).get("classification", "")
    if classification in ("SAME_DAY", "ONE_DAY", "TWO_DAY", "THREE_TO_FIVE_DAYS"):
        return "ja"

    return "nein"


def _description_ok(description_html: str) -> bool:
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
# Step 1: search by article number (with fallbacks)
# ---------------------------------------------------------------------------
async def _search_api(client: httpx.AsyncClient, query: str, first: int = 1) -> Tuple[List[Dict], str]:
    """Return (nodes, log_message). log_message describes the HTTP outcome."""
    variables = {
        "query": query,
        "searchQueryConfig": {},
        "first": first,
        "sortOrder": "RELEVANCE",
    }
    try:
        resp = await client.post(SEARCH_QUERY_URL, json={"variables": variables})
    except Exception as e:
        return [], f"search '{query}' EXC {type(e).__name__}: {e}"

    if resp.status_code != 200:
        snippet = resp.text[:120].replace("\n", " ")
        return [], f"search '{query}' HTTP {resp.status_code} ({snippet!r})"

    try:
        data = resp.json()
    except Exception as e:
        return [], f"search '{query}' JSON {type(e).__name__}: {e}"

    edges = (
        (data.get("data") or {})
        .get("shopSearch", {})
        .get("products", {})
        .get("edges", [])
    )
    nodes = [e.get("node") or {} for e in edges]
    return nodes, f"search '{query}' -> {len(nodes)} hit(s)"


# ---------------------------------------------------------------------------
# Step 2: product detail
# ---------------------------------------------------------------------------
async def _fetch_product_detail(client: httpx.AsyncClient, slug: str) -> Tuple[Dict, str]:
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
    try:
        resp = await client.post(DETAIL_QUERY_URL, json={"variables": variables})
    except Exception as e:
        return {}, f"detail EXC {type(e).__name__}: {e}"

    if resp.status_code != 200:
        snippet = resp.text[:120].replace("\n", " ")
        return {}, f"detail HTTP {resp.status_code} ({snippet!r})"

    try:
        return resp.json(), "detail OK"
    except Exception as e:
        return {}, f"detail JSON {type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# Step 3: description via offer query
# ---------------------------------------------------------------------------
async def _fetch_description(client: httpx.AsyncClient, product_numeric_id: int) -> Tuple[str, str]:
    query = (
        "query {"
        f" productsWithOfferDefault(productIds: [{product_numeric_id}]) {{"
        "   products { product { description } }"
        " }"
        "}"
    )
    try:
        resp = await client.post(OFFER_QUERY_URL, json={"query": query})
    except Exception as e:
        return "", f"desc EXC {type(e).__name__}: {e}"

    if resp.status_code != 200:
        return "", f"desc HTTP {resp.status_code}"

    try:
        data = resp.json()
        products = (
            data.get("data", {})
            .get("productsWithOfferDefault", {})
            .get("products", [])
        )
        if products:
            return (products[0].get("product") or {}).get("description") or "", "desc OK"
        return "", "desc empty"
    except Exception as e:
        return "", f"desc JSON {type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# Public: full product check
# ---------------------------------------------------------------------------
async def check_galaxus_product(product_id: str) -> Dict[str, Any]:
    log_parts: List[str] = []

    # mehrere Such-Varianten ausprobieren
    queries = [product_id]
    stripped_slash = re.sub(r"/\d+$", "", product_id)
    if stripped_slash and stripped_slash != product_id:
        queries.append(stripped_slash)
    no_slash = product_id.replace("/", " ")
    if no_slash not in queries:
        queries.append(no_slash)

    nodes: List[Dict] = []
    async with httpx.AsyncClient(timeout=20.0, headers=_HEADERS, follow_redirects=True) as client:
        for q in queries:
            nodes, msg = await _search_api(client, q)
            log_parts.append(msg)
            if nodes:
                break

        if not nodes:
            return {
                "url": "",
                "available": "nein",
                "images_ok": "nein",
                "videos_ok": "nein",
                "bullets_ok": "nein",
                "price_chf": "",
                "main_image_url": "",
                "log": " | ".join(log_parts),
            }

        node = nodes[0]
        slug = _slug_de(node.get("relativeUrl", ""))
        url = GALAXUS_BASE + slug if slug else ""
        product_numeric_id = _numeric_id_from_slug(slug)
        log_parts.append(f"slug={slug}")

        # Detail + Description parallel
        async def _empty():
            return "", "desc skipped (no numeric id)"

        detail_task = asyncio.create_task(_fetch_product_detail(client, slug))
        desc_task = asyncio.create_task(
            _fetch_description(client, product_numeric_id) if product_numeric_id else _empty()
        )

        detail_data, detail_msg = await detail_task
        log_parts.append(detail_msg)

        description, desc_msg = await desc_task
        log_parts.append(desc_msg)

    product = (detail_data.get("data") or {}).get("product") or {}

    gallery_count = (product.get("galleryImages") or {}).get("totalCount", 0)
    video_count = (product.get("videos") or {}).get("totalCount", 0)
    log_parts.append(f"gallery={gallery_count} videos={video_count}")

    available = _availability_ja_nein(product) if product else "nein"

    price_obj = product.get("price") or {}
    price_val = price_obj.get("amountInclusive")
    if price_val is None:
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
        "log": " | ".join(log_parts),
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
    Returns {"rank": int|"", "over_48": bool, "log": str}.
    """
    if not keyword or not product_id:
        return {"rank": "", "over_48": False, "log": "kw skipped (empty)"}

    log = ""
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
        except Exception as e:
            return {"rank": "", "over_48": False, "log": f"kw browser EXC {e}"}

        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=_HEADERS["User-Agent"],
            locale="de-CH",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = await context.new_page()

        try:
            await page.goto(
                GALAXUS_SEARCH.format(q=quote(keyword, safe="")),
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await _try_cookie_reject(page)
            await page.wait_for_timeout(800)

            for rank in range(1, 49):
                html = await page.content()
                if product_id in html:
                    log = f"kw '{keyword}' -> rank {rank}"
                    return {"rank": rank, "over_48": False, "log": log}
                await page.mouse.wheel(0, 1200)
                await page.wait_for_timeout(350)
            return {"rank": "", "over_48": True, "log": f"kw '{keyword}' >48"}
        except Exception as e:
            return {"rank": "", "over_48": False, "log": f"kw EXC {type(e).__name__}: {e}"}
        finally:
            await context.close()
            await browser.close()
