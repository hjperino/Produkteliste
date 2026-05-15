# worker/galaxus.py
"""
Galaxus product check via persisted GraphQL queries.

Anti-Block-Strategie:
  - Browser-realistische Headers (sec-ch-ua, sec-fetch-*)
  - Cookie-Warmup: zuerst /de aufrufen, Cookies einsammeln, dann erst APIs
  - Retry mit alternativem User-Agent bei 403/leerer Antwort
  - Detailliertes Logging in 'log' (landet in Excel-Spalte 'Debug')
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

DG_PORTAL = "22"

# Drei verschiedene User-Agents (zum Rotieren bei 403)
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def _base_headers(user_agent: str) -> Dict[str, str]:
    """Liefert browser-realistische Headers inkl. sec-ch-ua / sec-fetch-*."""
    return {
        "Accept": "*/*",
        "Accept-Language": "de-CH,de;q=0.9,en;q=0.7",
        "Content-Type": "application/json",
        "Origin": GALAXUS_BASE,
        "Referer": f"{GALAXUS_BASE}/de",
        "User-Agent": user_agent,
        "X-Dg-Portal": DG_PORTAL,
        # apollo client identifies us as a "browser-like" GraphQL client
        "apollographql-client-name": "shop-spa",
        "apollographql-client-version": "1.0.0",
        # client hints (Chrome 124)
        "sec-ch-ua": '"Chromium";v="124", "Not-A.Brand";v="99", "Google Chrome";v="124"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
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
# Cookie-Warmup
# ---------------------------------------------------------------------------
async def _warmup(client: httpx.AsyncClient) -> str:
    """
    Lädt die Galaxus-Startseite, damit das Server-seitige Anti-Bot uns Cookies
    setzt. Diese Cookies sind dann auf dem AsyncClient gespeichert und werden
    automatisch bei den folgenden API-Calls mitgesendet.
    """
    try:
        r = await client.get(f"{GALAXUS_BASE}/de", timeout=15.0)
        cookies = ";".join(client.cookies.keys()) or "none"
        return f"warmup HTTP {r.status_code}, cookies=[{cookies}]"
    except Exception as e:
        return f"warmup EXC {type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# Search (with auto-retry on 403 using different UA)
# ---------------------------------------------------------------------------
async def _search_once(client: httpx.AsyncClient, query: str, first: int) -> Tuple[List[Dict], str, int]:
    variables = {
        "query": query,
        "searchQueryConfig": {},
        "first": first,
        "sortOrder": "RELEVANCE",
    }
    try:
        resp = await client.post(SEARCH_QUERY_URL, json={"variables": variables})
    except Exception as e:
        return [], f"EXC {type(e).__name__}: {e}", 0

    status = resp.status_code
    if status != 200:
        snippet = resp.text[:80].replace("\n", " ")
        return [], f"HTTP {status} ({snippet!r})", status

    try:
        data = resp.json()
    except Exception as e:
        return [], f"JSON {type(e).__name__}: {e}", status

    edges = (
        (data.get("data") or {})
        .get("shopSearch", {})
        .get("products", {})
        .get("edges", [])
    )
    nodes = [e.get("node") or {} for e in edges]
    return nodes, f"OK {len(nodes)} hit(s)", status


# ---------------------------------------------------------------------------
# Product detail
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
        return {}, f"detail HTTP {resp.status_code}"

    try:
        return resp.json(), "detail OK"
    except Exception as e:
        return {}, f"detail JSON {type(e).__name__}: {e}"


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

    queries = [product_id]
    stripped_slash = re.sub(r"/\d+$", "", product_id)
    if stripped_slash and stripped_slash != product_id:
        queries.append(stripped_slash)
    no_slash = product_id.replace("/", " ")
    if no_slash not in queries:
        queries.append(no_slash)

    nodes: List[Dict] = []
    detail_data: Dict = {}
    description: str = ""

    # Bis zu zwei User-Agent-Varianten probieren bei 403
    for ua_idx, ua in enumerate(USER_AGENTS[:2]):
        headers = _base_headers(ua)
        async with httpx.AsyncClient(
            timeout=20.0,
            headers=headers,
            follow_redirects=True,
            http2=False,
        ) as client:

            # Warmup nur beim ersten Versuch ausführlich loggen
            wm = await _warmup(client)
            if ua_idx == 0:
                log_parts.append(wm)

            tried_any_search = False
            for q in queries:
                nodes, msg, status = await _search_once(client, q, first=1)
                log_parts.append(f"search '{q}' ua{ua_idx}: {msg}")
                tried_any_search = True
                if nodes:
                    break
                if status == 403:
                    # gleich abbrechen und mit nächstem UA neu versuchen
                    break

            if nodes:
                node = nodes[0]
                slug = _slug_de(node.get("relativeUrl", ""))
                product_numeric_id = _numeric_id_from_slug(slug)
                log_parts.append(f"slug={slug}")

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
                break  # erfolgreich, kein zweiter UA-Versuch nötig

            # nicht erfolgreich: nächster User-Agent
            if not tried_any_search:
                log_parts.append(f"ua{ua_idx}: no search executed")

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
    if not keyword or not product_id:
        return {"rank": "", "over_48": False, "log": "kw skipped (empty)"}

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-features=IsolateOrigins,site-per-process",
                ],
            )
        except Exception as e:
            return {"rank": "", "over_48": False, "log": f"kw browser EXC {e}"}

        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=USER_AGENTS[0],
            locale="de-CH",
            timezone_id="Europe/Zurich",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            "Object.defineProperty(navigator, 'languages', {get: () => ['de-CH','de','en']});"
            "Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});"
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
                    return {"rank": rank, "over_48": False, "log": f"kw '{keyword}' -> rank {rank}"}
                await page.mouse.wheel(0, 1200)
                await page.wait_for_timeout(350)
            return {"rank": "", "over_48": True, "log": f"kw '{keyword}' >48"}
        except Exception as e:
            return {"rank": "", "over_48": False, "log": f"kw EXC {type(e).__name__}: {e}"}
        finally:
            await context.close()
            await browser.close()
