# api/models.py
from __future__ import annotations
from pydantic import BaseModel
from typing import Literal, Optional

JobStatus = Literal["queued", "running", "done", "failed"]

class JobInfo(BaseModel):
    job_id: str
    status: JobStatus
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    total: int = 0
    done: int = 0
    failed: int = 0
    message: Optional[str] = None
