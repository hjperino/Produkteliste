#!/usr/bin/env python3
"""
Lokaler Diagnose-Test fuer den Produkteliste-Bot.

Zweck:
    Prueft, ob Galaxus + Toppreise + Philips von DEINEM Mac aus erreichbar sind.
    Wenn ja -> Render's Cloud-IP ist das Problem, nicht der Code.

Aufruf (aus dem Projektordner):
    python3 local_test.py

Setup (einmalig - siehe README am Ende der Datei):
    1. pip install -r requirements.txt
    2. python3 -m playwright install chromium
"""
from __future__ import annotations

import asyncio
import sys
import time
from typing import Any, Dict

# Wir importieren die echten Worker-Funktionen aus dem Projekt.
sys.path.insert(0, ".")

from app.worker.galaxus import check_galaxus_product, check_keyword_rank
from app.worker.toppreise import check_toppreise


# --- Test-Konfiguration: was wir pruefen ----------------------------------
TEST_PRODUCT_ID = "BRI921/00"
TEST_KEYWORD    = "IPL"


def _section(title: str) -> None:
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def _show(d: Dict[str, Any]) -> None:
    for k, v in d.items():
        if isinstance(v, str) and len(v) > 1200:
            v = v[:1200] + " [...]"
        print(f"  {k:18s} : {v!r}")


async def main() -> None:
    print(f"\nLokaler Diagnose-Test")
    print(f"  Produkt: {TEST_PRODUCT_ID}")
    print(f"  Keyword: {TEST_KEYWORD}")
    print(f"  Python:  {sys.version.split()[0]}")

    # ---------- 1. Galaxus Produktdaten ----------
    _section("1) Galaxus: check_galaxus_product")
    t0 = time.time()
    try:
        gx = await check_galaxus_product(TEST_PRODUCT_ID)
        print(f"  Dauer: {time.time()-t0:.1f}s")
        _show(gx)
    except Exception as e:
        print(f"  EXCEPTION: {type(e).__name__}: {e}")

    # ---------- 2. Toppreise ----------
    _section("2) Toppreise: check_toppreise")
    t0 = time.time()
    try:
        tp = await check_toppreise(TEST_PRODUCT_ID)
        print(f"  Dauer: {time.time()-t0:.1f}s")
        _show(tp)
    except Exception as e:
        print(f"  EXCEPTION: {type(e).__name__}: {e}")

    # ---------- 3. Keyword-Rang ----------
    _section("3) Galaxus: check_keyword_rank")
    t0 = time.time()
    try:
        kr = await check_keyword_rank(TEST_KEYWORD, TEST_PRODUCT_ID)
        print(f"  Dauer: {time.time()-t0:.1f}s")
        _show(kr)
    except Exception as e:
        print(f"  EXCEPTION: {type(e).__name__}: {e}")

    # ---------- Auswertung ----------
    _section("ZUSAMMENFASSUNG")
    gx_ok = gx.get("price_chf") not in ("", None) if isinstance(gx, dict) else False
    tp_ok = tp.get("best_price_chf") not in ("", None) if isinstance(tp, dict) else False
    kr_ok = bool(kr.get("rank")) if isinstance(kr, dict) else False

    print(f"  Galaxus Preis gefunden? : {'JA' if gx_ok else 'NEIN'}")
    print(f"  Toppreise Preis gefunden?: {'JA' if tp_ok else 'NEIN'}")
    print(f"  Keyword-Rang gefunden?   : {'JA' if kr_ok else 'NEIN'}")

    if gx_ok and tp_ok and kr_ok:
        print("\n  -> Lokal funktioniert alles. Render's IP ist die Blockade.")
    elif not gx_ok:
        print("\n  -> Auch lokal kein Galaxus-Zugriff. Pruefe die [GX]-Log-Zeilen oben.")
    else:
        print("\n  -> Gemischt. Schau die Details oben.")


if __name__ == "__main__":
    asyncio.run(main())


# -------------------------------------------------------------------------
# Setup-Hilfe (falls noetig):
#
#   1. Python pruefen:
#        python3 --version    # sollte >= 3.10 sein
#
#   2. Im Projektordner ein virtuelles Environment anlegen (empfohlen):
#        python3 -m venv .venv
#        source .venv/bin/activate
#
#   3. Dependencies installieren:
#        pip install -r requirements.txt
#
#   4. Playwright-Browser installieren (einmalig, ~150 MB):
#        python3 -m playwright install chromium
#
#   5. Test starten:
#        python3 local_test.py
#
# Erwartete Laufzeit: ca. 30-60 Sekunden.
# -------------------------------------------------------------------------
