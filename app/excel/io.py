# excel/io.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List
import io
import openpyxl

HEADER_ROW = 3
DATA_START_ROW = 4

def normalize_header(value) -> str:
    """
    - Converts to string
    - Strips leading/trailing whitespace
    - Collapses multiple internal spaces
    """
    if value is None:
        return ""
    return " ".join(str(value).split())

@dataclass
class InputRow:
    row_index: int
    product_id: str
    keyword: str

def load_workbook_from_bytes(xlsx_bytes: bytes):
    return openpyxl.load_workbook(io.BytesIO(xlsx_bytes))

def workbook_to_bytes(wb) -> bytes:
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

def map_headers(ws) -> Dict[str, int]:
    headers: Dict[str, int] = {}
    for col in range(1, ws.max_column + 1):
        h = normalize_header(ws.cell(HEADER_ROW, col).value)
        if h:
            headers[h] = col
    return headers

def read_inputs(ws, headers: Dict[str, int]) -> List[InputRow]:
    if "ID" not in headers:
        raise ValueError("Missing required column header 'ID' in row 3.")

    id_col = headers["ID"]
    keyword_col = headers.get("Keyword")  # optional, but present in your template

    inputs: List[InputRow] = []
    for r in range(DATA_START_ROW, ws.max_row + 1):
        pid = normalize_header(ws.cell(r, id_col).value)
        if not pid:
            continue
        kw = normalize_header(ws.cell(r, keyword_col).value) if keyword_col else ""
        inputs.append(InputRow(row_index=r, product_id=pid, keyword=kw))
    return inputs

def write_outputs(ws, headers: Dict[str, int], results_by_row: Dict[int, dict]) -> None:
    """
    Writes results into your updated columns.

    IMPORTANT CHANGES:
    - 'Available' now stores the availability TEXT extracted from icon tooltip/hidden text.
    - 'Platzierung Keyword' is removed -> we do NOT write keyword rank to Excel.
    """

    col_available = headers.get("Available")
    col_img = headers.get("Min. 6 Bilder")
    col_vid = headers.get("1- 3 Videos")
    col_bul = headers.get("Produktbeschr")
    # col_mainimg = headers.get("Gutes Mainbild")  # not filled by our steps
    col_price_gx = headers.get("Preis Galaxus")
    col_price_mp = headers.get("Preis Marktpreis Main")
    col_vendor_mp = headers.get("Marktpreis Anbieter Main")

    for row_idx, res in results_by_row.items():
        if col_available:
            ws.cell(row_idx, col_available).value = res.get("availability_text", "")
        if col_img:
            ws.cell(row_idx, col_img).value = "ja" if res.get("images_ok") else "nein"
        if col_vid:
            ws.cell(row_idx, col_vid).value = "ja" if res.get("videos_ok") else "nein"
        if col_bul:
            ws.cell(row_idx, col_bul).value = "ja" if res.get("bullets_ok") else "nein"
        if col_price_gx:
            ws.cell(row_idx, col_price_gx).value = res.get("galaxus_price", "")
        if col_price_mp:
            ws.cell(row_idx, col_price_mp).value = res.get("toppreise_price", "")
        if col_vendor_mp:
            ws.cell(row_idx, col_vendor_mp).value = res.get("toppreise_vendor", "")
