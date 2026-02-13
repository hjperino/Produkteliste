# api/main.py
from __future__ import annotations
import os
import uuid
import base64
from datetime import datetime, timezone

from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.responses import Response, JSONResponse

from api.models import JobInfo
from storage.jsonbin import JsonBin
from worker.runner import run_job_excel

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

app = FastAPI(title="Produkteliste Bot API", version="1.0")

# --- ENV ---
JSONBIN_API_KEY = os.environ.get("JSONBIN_API_KEY", "")
JSONBIN_JOBS_BIN_ID = os.environ.get("JSONBIN_JOBS_BIN_ID", "")
JSONBIN_RESULTS_BIN_ID = os.environ.get("JSONBIN_RESULTS_BIN_ID", "")

if not (JSONBIN_API_KEY and JSONBIN_JOBS_BIN_ID and JSONBIN_RESULTS_BIN_ID):
    # You can still run locally without jsonbin if you want,
    # but on Render you should set these.
    pass

store = JsonBin(JSONBIN_API_KEY, JSONBIN_JOBS_BIN_ID, JSONBIN_RESULTS_BIN_ID)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/jobs", response_model=JobInfo)
async def create_job(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Please upload an .xlsx file")

    xlsx_bytes = await file.read()
    job_id = str(uuid.uuid4())

    job = JobInfo(
        job_id=job_id,
        status="queued",
        created_at=utc_now_iso(),
        total=0,
        done=0,
        failed=0,
    )
    store.put_job(job_id, job.model_dump())

    # initialize results record
    store.put_results(job_id, {
        "excel_bytes_b64": None,
        "results_by_row": {},
        "finished_at": None,
        "count": 0,
    })

    background_tasks.add_task(_run_job_task, job_id, xlsx_bytes)
    return job

def _run_job_task(job_id: str, xlsx_bytes: bytes) -> None:
    try:
        job = store.get_job(job_id) or {}
        job["status"] = "running"
        job["started_at"] = utc_now_iso()
        store.put_job(job_id, job)

        import asyncio
        result = asyncio.run(run_job_excel(xlsx_bytes))

        excel_b64 = base64.b64encode(result["excel_bytes"]).decode("ascii")
        store.put_results(job_id, {
            "excel_bytes_b64": excel_b64,
            "results_by_row": result["results_by_row"],  # includes keyword rank in JSON
            "finished_at": result["finished_at"],
            "count": result["count"],
        })

        job["status"] = "done"
        job["finished_at"] = result["finished_at"]
        job["total"] = result["count"]
        job["done"] = result["count"]
        store.put_job(job_id, job)

    except Exception as e:
        job = store.get_job(job_id) or {}
        job["status"] = "failed"
        job["finished_at"] = utc_now_iso()
        job["message"] = f"{type(e).__name__}: {e}"
        store.put_job(job_id, job)

@app.get("/jobs/{job_id}", response_model=JobInfo)
def get_job(job_id: str):
    job = store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobInfo(**job)

@app.get("/jobs/{job_id}/results")
def get_results(job_id: str):
    res = store.get_results(job_id)
    if not res:
        raise HTTPException(status_code=404, detail="Job not found")
    # hide excel payload in JSON response
    return JSONResponse({k: v for k, v in res.items() if k != "excel_bytes_b64"})

@app.get("/jobs/{job_id}/export.xlsx")
def download_excel(job_id: str):
    res = store.get_results(job_id)
    if not res or not res.get("excel_bytes_b64"):
        raise HTTPException(status_code=404, detail="No Excel available (job not done?)")

    xlsx_bytes = base64.b64decode(res["excel_bytes_b64"])
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="Produkteliste_checked_{job_id}.xlsx"'},
    )
