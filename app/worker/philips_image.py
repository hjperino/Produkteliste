# worker/philips_image.py
"""
Compare the Galaxus main image with the official Philips product image.

Flow:
  1. Open philips.ch and search for the article number
  2. Open the first product result
  3. Extract the main image URL
  4. Download both images (Galaxus + Philips)
  5. Ask Claude (Vision) whether they show the same product / picture
  6. Return "ja" or "nein"

Env vars
--------
ANTHROPIC_API_KEY   required, used for the Vision call
ANTHROPIC_MODEL     optional, default "claude-3-5-sonnet-latest"
"""
from __future__ import annotations

import base64
import os
import re
from typing import Any, Dict, Optional
from urllib.parse import quote

import httpx
from playwright.async_api import async_playwright, Page

PHILIPS_SEARCH = "https://www.philips.ch/c-s/search-results.html?q={q}"

_VISION_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
_ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"


# ---------------------------------------------------------------------------
# Philips scraping
# ---------------------------------------------------------------------------
async def _try_cookie_accept(page: Page) -> None:
    for sel in (
        "button:has-text('Akzeptieren')",
        "button:has-text('Alle akzeptieren')",
        "button:has-text('Einverstanden')",
        "#onetrust-accept-btn-handler",
    ):
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass


async def _get_philips_main_image(product_id: str) -> str:
    """Return the URL of the Philips product hero image, or '' if not found."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(locale="de-CH",
                                        viewport={"width": 1280, "height": 900})
        page = await ctx.new_page()
        try:
            await page.goto(
                PHILIPS_SEARCH.format(q=quote(product_id, safe="")),
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await _try_cookie_accept(page)
            await page.wait_for_timeout(800)

            # Erstes Produkt-Resultat öffnen
            link = page.locator(
                "a[href*='/c-p/'], a[href*='/product/'], a.product-name, a.product-tile"
            ).first
            if await link.count() > 0:
                try:
                    await link.click(timeout=8000)
                    await page.wait_for_load_state("domcontentloaded")
                    await page.wait_for_timeout(1000)
                except Exception:
                    pass

            # Hero-Image extrahieren
            for sel in (
                "img.product-hero__image",
                "div.product-hero img",
                "meta[property='og:image']",
                "img[itemprop='image']",
                "div.gallery img",
                "img[src*='/cp/']",
            ):
                loc = page.locator(sel).first
                try:
                    if await loc.count() == 0:
                        continue
                    if sel.startswith("meta"):
                        src = await loc.get_attribute("content")
                    else:
                        src = await loc.get_attribute("src")
                    if src and src.startswith("http"):
                        return src
                except Exception:
                    continue
            return ""
        finally:
            await ctx.close()
            await browser.close()


# ---------------------------------------------------------------------------
# Image download + base64
# ---------------------------------------------------------------------------
async def _download_b64(url: str) -> Optional[Dict[str, str]]:
    if not url:
        return None
    try:
        async with httpx.AsyncClient(timeout=20.0,
                                     follow_redirects=True,
                                     headers={"User-Agent": "Mozilla/5.0"}) as client:
            r = await client.get(url)
            r.raise_for_status()
            ct = r.headers.get("content-type", "image/jpeg").split(";")[0].strip()
            if ct not in ("image/jpeg", "image/png", "image/webp", "image/gif"):
                ct = "image/jpeg"
            return {"media_type": ct, "data": base64.b64encode(r.content).decode()}
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Vision compare
# ---------------------------------------------------------------------------
async def _vision_same_image(img_a: Dict[str, str], img_b: Dict[str, str]) -> bool:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return False

    prompt = (
        "Du siehst zwei Produktbilder. Antworte NUR mit 'ja' oder 'nein'. "
        "Antwort 'ja' wenn beide Bilder das gleiche Produkt aus derselben "
        "Perspektive zeigen (z.B. identisches Hero-Bild des Herstellers, "
        "ggf. mit kleinem Beschnitt oder Skalierung). Antwort 'nein' wenn "
        "Perspektive, Beleuchtung, Produktvariante oder Ausschnitt "
        "deutlich abweichen."
    )

    payload = {
        "model": _VISION_MODEL,
        "max_tokens": 8,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image", "source": {
                    "type": "base64",
                    "media_type": img_a["media_type"],
                    "data": img_a["data"],
                }},
                {"type": "image", "source": {
                    "type": "base64",
                    "media_type": img_b["media_type"],
                    "data": img_b["data"],
                }},
            ],
        }],
    }
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(_ANTHROPIC_API_URL, headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()
            text = ""
            for block in data.get("content", []):
                if block.get("type") == "text":
                    text += block.get("text", "")
            return bool(re.match(r"\s*ja\b", text.lower()))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
async def check_mainbild(product_id: str, galaxus_image_url: str) -> Dict[str, Any]:
    """
    Returns:
      mainbild_ok   : "ja" / "nein"
      philips_url   : str   (the Philips image URL we found)
      notes         : str
    """
    notes = ""
    philips_url = ""
    try:
        philips_url = await _get_philips_main_image(product_id)
    except Exception as e:
        notes = f"Philips-Scrape: {e}"

    if not galaxus_image_url or not philips_url:
        return {
            "mainbild_ok": "nein",
            "philips_url": philips_url,
            "notes": notes or "Bild fehlt (Galaxus oder Philips)",
        }

    img_gx = await _download_b64(galaxus_image_url)
    img_ph = await _download_b64(philips_url)
    if not img_gx or not img_ph:
        return {
            "mainbild_ok": "nein",
            "philips_url": philips_url,
            "notes": notes or "Download eines Bildes fehlgeschlagen",
        }

    same = await _vision_same_image(img_gx, img_ph)
    return {
        "mainbild_ok": "ja" if same else "nein",
        "philips_url": philips_url,
        "notes": notes,
    }
