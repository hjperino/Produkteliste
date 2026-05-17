# worker/toppreise.py
"""
Toppreise check: günstigster Preis unter erlaubten Anbietern.

Flow:
  1. Navigate to /produktsuche?q=<id>  (deutsche Suche)
  2. Cookie-Banner (Google Funding Choices) wegklicken
  3. Aus Suchresultaten den richtigen Detail-Link wählen (Artikelnummer im href)
  4. Detail-Seite laden, HTML auslesen
  5. Plugin_Offer-Blöcke parsen:
       - Shop-Name aus <img src*='/logo/' alt='...'>
       - Preis aus <div class='Plugin_Price'>123.45</div>
  6. Erlaubte Anbieter filtern, günstigsten zurückgeben.
"""
from __future__ import annotations

import re
import time
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import quote

from playwright.async_api import async_playwright, Page

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TOPPREISE_SEARCH = "https://www.toppreise.ch/produktsuche?q={q}"

# Erlaubte Anbieter (Pattern lowercase → Anzeigename)
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

CHROMIUM_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-http2",
    "--disable-features=IsolateOrigins,site-per-process",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _vendor_canonical(shop_name: str) -> str:
    """Mappe einen Shop-Logo-alt-Text auf den kanonischen Anzeigenamen."""
    if not shop_name:
        return ""
    s = shop_name.lower()
    for pat, canon in ALLOWED_VENDORS:
        if pat in s:
            return canon
    return ""


def _parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"([\d'.,]+)", text)
    if not m:
        return None
    s = m.group(1).replace("'", "")
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


async def _try_cookie_accept(page: Page) -> None:
    # Toppreise nutzt Google Funding Choices
    for sel in (
        ".fc-cta-consent",
        ".fc-cta-do-not-consent",
        "button.fc-button:has-text('Akzeptieren')",
        "button:has-text('Akzeptieren')",
        "button:has-text('Alle akzeptieren')",
        "button:has-text('Einverstanden')",
        "button:has-text('Accept')",
        "#onetrust-accept-btn-handler",
    ):
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await page.wait_for_timeout(500)
                return
        except Exception:
            pass


# ---------------------------------------------------------------------------
# HTML-Parser für die Detail-Seite
# ---------------------------------------------------------------------------
def _parse_offers_html(html: str) -> List[Tuple[str, float]]:
    """
    Extrahiert (shop_name, price) Paare aus dem HTML der Toppreise-Detail-Seite.

    Struktur jedes Angebots:
      <div ... class="Plugin_Offer ... productDetailsOfferList ...">
        ...
        <img src="//imgsrv.toppreise.ch/logo/<id>@1x" alt="<Shop-Name>" ...>
        ...
        <div class="currency">CHF</div>
        <div class="Plugin_Price">  204.85  </div>
        ...
      </div>
    """
    offers: List[Tuple[str, float]] = []

    starts = [m.start() for m in re.finditer(
        r'<[^>]+class="[^"]*Plugin_Offer[^"]*"[^>]*>', html)]
    if not starts:
        return offers
    starts.append(len(html))

    for i in range(len(starts) - 1):
        chunk = html[starts[i]:starts[i+1]]
        if 'priceContainer' not in chunk:
            continue

        # Preis aus Plugin_Price-div
        pm = re.search(
            r'class="[^"]*Plugin_Price[^"]*"[^>]*>\s*([\d\'.,]+)\s*</',
            chunk,
        )
        if not pm:
            continue
        price = _parse_price(pm.group(1))
        if price is None or price <= 0:
            continue

        # Shop-Name aus img mit /logo/ im src
        shop = ""
        for m in re.finditer(r'<img[^>]+>', chunk):
            tag = m.group(0)
            if "/logo/" not in tag and "imgsrv.toppreise.ch/logo" not in tag:
                continue
            am = re.search(r'alt="([^"]+)"', tag)
            if am:
                shop = am.group(1)
                break

        # Fallback: aus href="/shops/<Name>-s<id>" ableiten
        if not shop:
            m = re.search(r'href="/shops/([^?"]+)"', chunk)
            if m:
                shop = m.group(1).rsplit("-s", 1)[0].replace("-", " ")

        if shop:
            offers.append((shop, price))

    return offers


# ---------------------------------------------------------------------------
# Public
# ---------------------------------------------------------------------------
async def check_toppreise(product_id: str) -> Dict[str, Any]:
    log: List[str] = []

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
        except Exception as e:
            return {"best_price_chf": "", "vendor": "",
                    "log": f"tp launch EXC {type(e).__name__}: {e}"}

        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="de-CH",
            timezone_id="Europe/Zurich",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            "Object.defineProperty(navigator, 'languages', {get: () => ['de-CH','de','en']});"
        )
        page = await context.new_page()

        try:
            # 1. Suchergebnis-Seite oeffnen
            q = quote(product_id, safe="")
            search_url = TOPPREISE_SEARCH.format(q=q)
            try:
                resp = await page.goto(search_url, wait_until="commit", timeout=20000)
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except Exception:
                    pass
                log.append(f"tp search status={resp.status if resp else '?'}")
            except Exception as e:
                return {"best_price_chf": "", "vendor": "",
                        "log": f"tp goto EXC {type(e).__name__}: {e}"}

            await _try_cookie_accept(page)
            await page.wait_for_timeout(1500)

            # 2. Richtigen Produkt-Link bestimmen
            pid_lower = product_id.lower()
            pid_url = re.sub(r"[^a-z0-9]+", "-", pid_lower).strip("-")
            pid_short = pid_url.split("-")[0]

            all_links = await page.locator("a[href*='/preisvergleich/']").evaluate_all(
                "els => els.map(e => e.getAttribute('href'))"
            )
            log.append(f"tp candidates={len(all_links)}")

            chosen_href = ""
            for href in all_links:
                if href and pid_url and pid_url in href.lower():
                    chosen_href = href
                    log.append(f"tp matched full '{pid_url}'")
                    break
            if not chosen_href:
                for href in all_links:
                    if href and pid_short in href.lower():
                        chosen_href = href
                        log.append(f"tp matched short '{pid_short}'")
                        break
            if not chosen_href and all_links:
                chosen_href = all_links[0]
                log.append("tp fallback first link")

            if not chosen_href:
                return {"best_price_chf": "", "vendor": "",
                        "log": " | ".join(log) + " || no product link found"}

            # 3. Detail-Seite oeffnen
            detail_url = chosen_href if chosen_href.startswith("http") \
                else "https://www.toppreise.ch" + chosen_href
            try:
                await page.goto(detail_url, wait_until="commit", timeout=20000)
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except Exception:
                    pass
                await page.wait_for_timeout(2000)
                log.append(f"tp detail_loaded={detail_url[:80]}")
            except Exception as e:
                return {"best_price_chf": "", "vendor": "",
                        "log": " | ".join(log) + f" || detail goto EXC {e}"}

            # 4. HTML-Dump (für Diagnose, optional)
            try:
                ts = int(time.time())
                dump_path = f"/tmp/toppreise_{ts}_{re.sub(r'[^A-Za-z0-9]+','_',product_id)}.html"
                content = await page.content()
                with open(dump_path, "w", encoding="utf-8") as f:
                    f.write(content)
                log.append(f"tp html_dump={dump_path}")
            except Exception as e:
                content = await page.content()
                log.append(f"tp dump skip ({e})")

            # 5. Offers parsen
            offers = _parse_offers_html(content)
            log.append(f"tp offers_parsed={len(offers)}")
            if not offers:
                return {"best_price_chf": "", "vendor": "", "log": " | ".join(log)}

            # Erste 3 Offers als Sample fuer Debug
            samples = [f"{s}@{p}" for s, p in offers[:3]]
            log.append("tp samples: " + " ; ".join(samples))

            # 6. Filter + günstigsten finden
            allowed: List[Tuple[str, float]] = []
            for shop, price in offers:
                canon = _vendor_canonical(shop)
                if canon:
                    allowed.append((canon, price))
            log.append(f"tp allowed_offers={len(allowed)}")

            if not allowed:
                return {"best_price_chf": "", "vendor": "", "log": " | ".join(log)}

            vendor, best = min(allowed, key=lambda x: x[1])
            log.append(f"tp best={vendor}@{best}")
            return {"best_price_chf": best, "vendor": vendor, "log": " | ".join(log)}
        finally:
            await context.close()
            await browser.close()
