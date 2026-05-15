# worker/runner.py
"""
Orchestrates one full Excel run:
  - read inputs (ID + optional Keyword) from sheet
  - for each product: Galaxus check, Mainbild compare, Toppreise, Keyword rank
  - write results back into the same sheet
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict

from app.excel.io import (
    load_workbook_from_bytes,
    map_headers,
    read_inputs,
    write_outputs,
    workbook_to_bytes,
)
from app.worker.galaxus import check_galaxus_product, check_keyword_rank
from app.worker.philips_image import check_mainbild
from app.worker.toppreise import check_toppreise


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_result() -> Dict[str, Any]:
    return {
        "available":        "nein",
        "images_ok":        "nein",
        "videos_ok":        "nein",
        "bullets_ok":       "nein",
        "mainbild_ok":      "nein",
        "keyword_rank":     "",
        "galaxus_price":    "",
        "toppreise_price":  "",
        "toppreise_vendor": "",
        "galaxus_url":      "",
        "notes":            "",
        "checked_at":       _iso_now(),
    }


async def run_job_excel(xlsx_bytes: bytes) -> Dict[str, Any]:
    wb = load_workbook_from_bytes(xlsx_bytes)
    ws = wb.active

    headers = map_headers(ws)
    inputs = read_inputs(ws, headers)

    results_by_row: Dict[int, dict] = {}
    sem = asyncio.Semaphore(2)  # konservative Parallelität gegen Rate-Limits

    async def process_one(inp):
        async with sem:
            res = _empty_result()
            notes_parts = []

            # --- 1. Galaxus product data ---
            try:
                gx = await check_galaxus_product(inp.product_id)
                res["galaxus_url"]   = gx.get("url", "")
                res["available"]     = gx.get("available", "nein")
                res["images_ok"]     = gx.get("images_ok", "nein")
                res["videos_ok"]     = gx.get("videos_ok", "nein")
                res["bullets_ok"]    = gx.get("bullets_ok", "nein")
                res["galaxus_price"] = gx.get("price_chf", "")
                gx_main_url          = gx.get("main_image_url", "")
                if gx.get("notes"):
                    notes_parts.append(f"GX: {gx['notes']}")
            except Exception as e:
                gx_main_url = ""
                notes_parts.append(f"GX ERROR: {type(e).__name__}: {e}")

            # --- 2. Mainbild vergleich (philips.ch vs galaxus) ---
            try:
                mb = await check_mainbild(inp.product_id, gx_main_url)
                res["mainbild_ok"] = mb.get("mainbild_ok", "nein")
                if mb.get("notes"):
                    notes_parts.append(f"MB: {mb['notes']}")
            except Exception as e:
                notes_parts.append(f"MB ERROR: {type(e).__name__}: {e}")

            # --- 3. Keyword rank (nur wenn Keyword vorhanden) ---
            if inp.keyword:
                try:
                    kr = await check_keyword_rank(inp.keyword, inp.product_id)
                    rank = kr.get("rank", "")
                    res["keyword_rank"] = rank if rank != "" else (">48" if kr.get("over_48") else "")
                except Exception as e:
                    notes_parts.append(f"KW ERROR: {type(e).__name__}: {e}")

            # --- 4. Toppreise ---
            try:
                tp = await check_toppreise(inp.product_id)
                res["toppreise_price"]  = tp.get("best_price_chf", "")
                res["toppreise_vendor"] = tp.get("vendor", "")
            except Exception as e:
                notes_parts.append(f"TP ERROR: {type(e).__name__}: {e}")

            res["notes"] = " | ".join(notes_parts)
            results_by_row[inp.row_index] = res
            await asyncio.sleep(0.8)  # pacing

    await asyncio.gather(*(process_one(i) for i in inputs))

    write_outputs(ws, headers, results_by_row)
    out_bytes = workbook_to_bytes(wb)

    return {
        "count":          len(inputs),
        "results_by_row": results_by_row,
        "excel_bytes":    out_bytes,
        "finished_at":    _iso_now(),
    }
