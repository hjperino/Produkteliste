# storage/jsonbin.py
from __future__ import annotations
import requests
from typing import Any, Dict, Optional

class JsonBin:
    """
    Simple approach:
    - Each bin stores a dict of {job_id: payload}
    - For moderate volume, read-modify-write is fine.
    """

    def __init__(self, api_key: str, jobs_bin_id: str, results_bin_id: str):
        self.api_key = api_key
        self.jobs_bin_id = jobs_bin_id
        self.results_bin_id = results_bin_id
        self.base = "https://api.jsonbin.io/v3"

    def _headers(self) -> Dict[str, str]:
        return {"X-Master-Key": self.api_key, "Content-Type": "application/json"}

    def _read_bin_record(self, bin_id: str) -> Dict[str, Any]:
        r = requests.get(f"{self.base}/b/{bin_id}/latest", headers=self._headers(), timeout=30)
        r.raise_for_status()
        rec = r.json().get("record")
        return rec if isinstance(rec, dict) else {}

    def _write_bin_record(self, bin_id: str, record: Dict[str, Any]) -> None:
        r = requests.put(f"{self.base}/b/{bin_id}", headers=self._headers(), json=record, timeout=30)
        r.raise_for_status()

    def _upsert(self, bin_id: str, key: str, value: Dict[str, Any]) -> None:
        record = self._read_bin_record(bin_id)
        record[key] = value
        self._write_bin_record(bin_id, record)

    def _get(self, bin_id: str, key: str) -> Optional[Dict[str, Any]]:
        record = self._read_bin_record(bin_id)
        v = record.get(key)
        return v if isinstance(v, dict) else None

    # Jobs
    def put_job(self, job_id: str, payload: Dict[str, Any]) -> None:
        self._upsert(self.jobs_bin_id, job_id, payload)

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        return self._get(self.jobs_bin_id, job_id)

    # Results
    def put_results(self, job_id: str, payload: Dict[str, Any]) -> None:
        self._upsert(self.results_bin_id, job_id, payload)

    def get_results(self, job_id: str) -> Optional[Dict[str, Any]]:
        return self._get(self.results_bin_id, job_id)
