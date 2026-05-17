# worker/galaxus.py
"""
Galaxus product check via HTTP only.

Strategy
--------
1. Search by article number via persisted GraphQL search query.
2. Fetch the product page as raw HTML via httpx (same Cookies/Headers).
   Parse the embedded __NEXT_DATA__ JSON for structured data.
3. Keyword rank: a single search-API call with first=48; we compare
   databaseId of every hit against the target product's databaseId.

No Playwright involved here -> faster, more robust on Akamai-protected pages.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

# ---------------------------------------------------------------------------
# Endpoints / constants
# ---------------------------------------------------------------------------
GALAXUS_BASE     = "https://www.galaxus.ch"
SEARCH_QUERY_ID  = "1550dffd10716adbe05c612c391fac32"
SEARCH_QUERY_URL = f"{GALAXUS_BASE}/graphql/o/{SEARCH_QUERY_ID}/useSearchDataQuery"
DG_PORTAL        = "22"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def _api_headers(ua: str) -> Dict[str, str]:
    """Headers für GraphQL-Calls (cors, json)."""
    return {
        "Accept": "*/*",
        "Accept-Language": "de-CH,de;q=0.9,en;q=0.7",
        "Content-Type": "application/json",
        "Origin": GALAXUS_BASE,
        "Referer": f"{GALAXUS_BASE}/de",
        "User-Agent": ua,
        "X-Dg-Portal": DG_PORTAL,
        "apollographql-client-name": "shop-spa",
        "apollographql-client-version": "1.0.0",
        "sec-ch-ua": '"Chromium";v="124", "Not-A.Brand";v="99", "Google Chrome";v="124"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }


def _html_headers(ua: str) -> Dict[str, str]:
    """Headers für GET auf HTML-Seiten."""
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "de-CH,de;q=0.9,en;q=0.7",
        "User-Agent": ua,
        "sec-ch-ua": '"Chromium";v="124", "Not-A.Brand";v="99", "Google Chrome";v="124"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _slug_de(relative_url: str) -> str:
    return re.sub(r"^/en/", "/de/", relative_url or "")


def _clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _description_ok(text: str) -> bool:
    parts = [p.strip() for p in re.split(r"\n+", text or "") if p.strip()]
    long_blocks = [p for p in parts if len(p) >= 300]
    short_blocks = [p for p in parts if 20 <= len(p) <= 400]
    return bool(long_blocks) and len(short_blocks) >= 5


# ---------------------------------------------------------------------------
# Warmup + Search
# ---------------------------------------------------------------------------
async def _warmup(client: httpx.AsyncClient, ua: str) -> str:
    try:
        r = await client.get(f"{GALAXUS_BASE}/de", headers=_html_headers(ua), timeout=15.0)
        return f"warmup HTTP {r.status_code} cookies=[{';'.join(client.cookies.keys()) or 'none'}]"
    except Exception as e:
        return f"warmup EXC {type(e).__name__}: {e}"


async def _search_once(client: httpx.AsyncClient, ua: str, query: str, first: int = 1) -> Tuple[List[Dict], str, int]:
    variables = {"query": query, "searchQueryConfig": {}, "first": first, "sortOrder": "RELEVANCE"}
    try:
        r = await client.post(SEARCH_QUERY_URL, headers=_api_headers(ua), json={"variables": variables})
    except Exception as e:
        return [], f"EXC {type(e).__name__}: {e}", 0

    if r.status_code != 200:
        snippet = r.text[:60].replace("\n", " ")
        return [], f"HTTP {r.status_code} ({snippet!r})", r.status_code

    try:
        data = r.json()
    except Exception as e:
        return [], f"JSON {e}", r.status_code

    edges = (data.get("data") or {}).get("shopSearch", {}).get("products", {}).get("edges", [])
    nodes = [e.get("node") or {} for e in edges]
    return nodes, f"OK {len(nodes)} hit(s)", r.status_code


# ---------------------------------------------------------------------------
# Product page HTML fetch + __NEXT_DATA__ parse
# ---------------------------------------------------------------------------
def _extract_next_data(html: str) -> Optional[Dict]:
    """
    Galaxus rendert mit Next.js. Das eingebettete JSON-Script
    <script id="__NEXT_DATA__" type="application/json">{...}</script>
    enthaelt alle Produktdaten.
    """
    m = re.search(
        r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html, re.S | re.I,
    )
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _find_first(obj: Any, key: str) -> Any:
    """Erste Vorkommen von `key` irgendwo in einem geschachtelten JSON-Objekt."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = _find_first(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_first(item, key)
            if found is not None:
                return found
    return None


def _all_values(obj: Any, key: str) -> List[Any]:
    """Alle Vorkommen von `key` in einem geschachtelten JSON-Objekt."""
    out: List[Any] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                out.append(v)
            out.extend(_all_values(v, key))
    elif isinstance(obj, list):
        for item in obj:
            out.extend(_all_values(item, key))
    return out


async def _fetch_product_html_data(client: httpx.AsyncClient, ua: str, slug: str) -> Tuple[Dict[str, Any], str]:
    url = GALAXUS_BASE + slug
    try:
        r = await client.get(url, headers=_html_headers(ua), timeout=30.0)
    except Exception as e:
        return {}, f"page EXC {type(e).__name__}: {e}"

    if r.status_code != 200:
        return {}, f"page HTTP {r.status_code} len={len(r.text)}"

    html = r.text
    log_parts = [f"page HTTP 200 len={len(html)}"]

    next_data = _extract_next_data(html)
    if not next_data:
        log_parts.append("no __NEXT_DATA__ found")
        return {"_html_only": True, "html": html}, " | ".join(log_parts)

    # ---- Gallery images count ----
    # Galaxus typischerweise: product.galleryImages.totalCount oder ähnlich
    gallery_count = 0
    gallery_obj = _find_first(next_data, "galleryImages")
    if isinstance(gallery_obj, dict):
        gallery_count = gallery_obj.get("totalCount") or len(gallery_obj.get("edges") or [])

    # Fallback: alle "totalCount" sammeln, höchsten image-relevanten Wert nehmen
    if not gallery_count:
        for img_key in ("images", "productImages", "productGallery"):
            obj = _find_first(next_data, img_key)
            if isinstance(obj, dict):
                gallery_count = obj.get("totalCount") or len(obj.get("edges") or obj.get("nodes") or [])
                if gallery_count:
                    break

    # ---- Videos count ----
    video_count = 0
    videos_obj = _find_first(next_data, "videos")
    if isinstance(videos_obj, dict):
        video_count = videos_obj.get("totalCount") or len(videos_obj.get("edges") or [])

    # ---- Availability ----
    availability_text = ""
    avail = _find_first(next_data, "availability")
    if isinstance(avail, dict):
        mail_detail = avail.get("mailDetail") or {}
        stock = mail_detail.get("stockDetails") or {}
        status = stock.get("status", "")
        stock_count = stock.get("stockCount")
        if status in ("IN_STOCK", "LOW_STOCK") and stock_count is not None and stock_count > 0:
            availability_text = f"{stock_count} Stück am Lager"
        elif status == "OUT_OF_STOCK":
            availability_text = "nicht am Lager"
        else:
            classification = (avail.get("mail") or {}).get("classification", "")
            if classification in ("SAME_DAY", "ONE_DAY", "TWO_DAY", "THREE_TO_FIVE_DAYS"):
                availability_text = "verfügbar"
            else:
                availability_text = ""

    # Fallback: regex über HTML-Body
    if not availability_text:
        m = re.search(r"(\d+)\s*St[üu]ck am Lager", html, re.I)
        if m and int(m.group(1)) > 0:
            availability_text = f"{m.group(1)} Stück am Lager"
        elif re.search(r"nicht am Lager", html, re.I):
            availability_text = "nicht am Lager"

    available = "ja" if ("am Lager" in availability_text and "nicht" not in availability_text
                         or availability_text == "verfügbar") else "nein"

    # ---- Description ----
    description = ""
    desc_obj = _find_first(next_data, "description")
    if isinstance(desc_obj, str):
        description = desc_obj
    elif isinstance(desc_obj, dict):
        description = desc_obj.get("text") or desc_obj.get("html") or ""

    # ---- Main image URL ----
    main_image_url = ""
    if isinstance(gallery_obj, dict):
        edges = gallery_obj.get("edges") or []
        if edges:
            node = edges[0].get("node") or {}
            for k in ("largeUrl", "url", "originalUrl", "highResUrl"):
                if node.get(k):
                    main_image_url = node[k]
                    break
    # Fallback: og:image
    if not main_image_url:
        m = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)', html)
        if m:
            main_image_url = m.group(1)

    log_parts.append(
        f"gallery={gallery_count} videos={video_count} avail='{availability_text}' "
        f"desc_len={len(description)} img={'yes' if main_image_url else 'no'}"
    )

    return {
        "available":      available,
        "images_count":   gallery_count,
        "videos_count":   video_count,
        "description":    description,
        "main_image_url": main_image_url,
    }, " | ".join(log_parts)


# ---------------------------------------------------------------------------
# Public: full product check
# ---------------------------------------------------------------------------
async def check_galaxus_product(product_id: str) -> Dict[str, Any]:
    log_parts: List[str] = []

    queries = [product_id]
    stripped = re.sub(r"/\d+$", "", product_id)
    if stripped and stripped != product_id:
        queries.append(stripped)
    no_slash = product_id.replace("/", " ")
    if no_slash not in queries:
        queries.append(no_slash)

    nodes: List[Dict] = []
    slug = ""
    price_chf: Any = ""

    # Suche via API
    ua_used = USER_AGENTS[0]
    async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
        for ua_idx, ua in enumerate(USER_AGENTS):
            log_parts.append(await _warmup(client, ua)) if ua_idx == 0 else None
            for q in queries:
                nodes, msg, status = await _search_once(client, ua, q)
                log_parts.append(f"search '{q}' ua{ua_idx}: {msg}")
                if nodes:
                    ua_used = ua
                    break
                if status == 403:
                    break
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
        price_val = (node.get("price") or {}).get("amountInclusive")
        try:
            price_chf = float(price_val) if price_val is not None else ""
        except (TypeError, ValueError):
            price_chf = ""
        log_parts.append(f"slug={slug} price={price_chf}")

        # Produktseite via HTTP holen
        page_data, page_log = await _fetch_product_html_data(client, ua_used, slug)
        log_parts.append(page_log)

    url = GALAXUS_BASE + slug
    images_count = page_data.get("images_count", 0) or 0
    videos_count = page_data.get("videos_count", 0) or 0
    description = page_data.get("description", "") or ""

    return {
        "url": url,
        "available": page_data.get("available", "nein"),
        "images_ok": "ja" if images_count >= 6 else "nein",
        "videos_ok": "ja" if videos_count >= 1 else "nein",
        "bullets_ok": "ja" if _description_ok(description) else "nein",
        "price_chf": price_chf,
        "main_image_url": page_data.get("main_image_url", ""),
        "log": " | ".join(log_parts),
    }


# ---------------------------------------------------------------------------
# Public: keyword ranking via Search API
# ---------------------------------------------------------------------------
async def check_keyword_rank(keyword: str, product_id: str) -> Dict[str, Any]:
    """
    Suchen via API:
      1. Hole erst databaseId von `product_id`
      2. Suche mit `keyword`, first=48
      3. Schau in welcher Position databaseId vorkommt
    """
    if not keyword or not product_id:
        return {"rank": "", "over_48": False, "log": "kw skipped (empty)"}

    log_parts: List[str] = []
    ua = USER_AGENTS[0]

    async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
        log_parts.append(await _warmup(client, ua))

        # Phase 1: Target databaseId finden
        nodes, msg, _ = await _search_once(client, ua, product_id)
        log_parts.append(f"target search '{product_id}': {msg}")
        if not nodes:
            return {"rank": "", "over_48": False, "log": " | ".join(log_parts)}
        target_id = nodes[0].get("databaseId")
        if not target_id:
            return {"rank": "", "over_48": False, "log": " | ".join(log_parts) + " | no databaseId"}
        log_parts.append(f"target databaseId={target_id}")

        # Phase 2: Keyword-Suche
        nodes, msg, _ = await _search_once(client, ua, keyword, first=48)
        log_parts.append(f"keyword search '{keyword}': {msg}")
        if not nodes:
            return {"rank": "", "over_48": False, "log": " | ".join(log_parts)}

        # Phase 3: Position finden
        for rank, node in enumerate(nodes, start=1):
            if node.get("databaseId") == target_id:
                log_parts.append(f"kw '{keyword}' -> rank {rank}")
                return {"rank": rank, "over_48": False, "log": " | ".join(log_parts)}

        log_parts.append(f"kw '{keyword}' >48")
        return {"rank": "", "over_48": True, "log": " | ".join(log_parts)}
