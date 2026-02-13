# worker/runner.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, Any
import asyncio

from excel.io import load_workbook_from_bytes, map_headers, read_inputs, write_outputs, workbook_to_bytes
from worker.galaxus import check_galaxus_product, check_keyword_rank
from worker.toppreise import check_toppreise

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def run_job_excel(xlsx_bytes: bytes) -> Dict[str, Any]:
    wb = load_workbook_from_bytes(xlsx_bytes)
    ws = wb.active

    headers = map_headers(ws)
    inputs = read_inputs(ws, headers)

    results_by_row: Dict[int, dict] = {}

    sem = asyncio.Semaphore(2)  # conservative concurrency to reduce blocking

    async def process_one(inp):
        async with sem:
            res = {
                "availability_text": "",
                "images_ok": False,
                "videos_ok": False,
                "bullets_ok": False,
                "galaxus_price": "",
                "galaxus_url": "",
                "toppreise_price": "",
                "toppreise_vendor": "",
                # stored in JSON only:
                "keyword": inp.keyword,
                "keyword_rank": "",
                "keyword_over_48": False,
                "checked_at": utc_now_iso(),
                "notes": "",
            }

            try:
                gx = await check_galaxus_product(inp.product_id)
                res["galaxus_url"] = gx.get("url", "")
                res["availability_text"] = gx.get("availability_text", "")

                res["images_ok"] = (gx.get("images_count", 0) >= 6)
                res["videos_ok"] = (gx.get("videos_count", 0) >= 1)
                res["bullets_ok"] = bool(gx.get("bullets_ok", False))
                res["galaxus_price"] = gx.get("price_chf", "")

                if inp.keyword:
                    rk = await check_keyword_rank(inp.keyword, inp.product_id)
                    res["keyword_rank"] = rk.get("rank", "")
                    res["keyword_over_48"] = bool(rk.get("over_48", False))

                tp = await check_toppreise(inp.product_id)
                res["toppreise_price"] = tp.get("best_price_chf", "")
                res["toppreise_vendor"] = tp.get("vendor", "")

            except Exception as e:
                res["notes"] = f"ERROR: {type(e).__name__}: {e}"

            results_by_row[inp.row_index] = res

            # pacing
            await asyncio.sleep(1.2)

    await asyncio.gather(*(process_one(i) for i in inputs))

    # write to Excel
    write_outputs(ws, headers, results_by_row)
    out_bytes = workbook_to_bytes(wb)

    return {
        "count": len(inputs),
        "results_by_row": results_by_row,
        "excel_bytes": out_bytes,
        "finished_at": utc_now_iso(),
    }
