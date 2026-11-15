"""Bounded, read-only access to a single completed or failed Spark run."""
import json
from pathlib import Path
import re

from fastapi import HTTPException

RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
MAX_SUMMARY_BYTES = 1024 * 1024


def read_run_summary(root: Path, run_id: str) -> dict:
    if not RUN_ID.fullmatch(run_id):
        raise HTTPException(status_code=400, detail="Invalid run ID")
    if not root.is_dir():
        raise HTTPException(status_code=503, detail="Evidence directory unavailable")

    directory = root / run_id
    if directory.is_symlink() or not directory.is_dir():
        raise HTTPException(status_code=404, detail="Run not found")
    summary = directory / "summary.json"
    if summary.is_symlink() or not summary.is_file():
        raise HTTPException(status_code=404, detail="Run summary not found")

    try:
        with summary.open("rb") as stream:
            raw = stream.read(MAX_SUMMARY_BYTES + 1)
        if len(raw) > MAX_SUMMARY_BYTES:
            raise ValueError("Summary exceeds size limit")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Summary must be a JSON object")
        if type(data.get("success")) is not bool:
            raise ValueError("Missing or invalid success status")
        if type(data.get("accounting_complete")) is not bool:
            raise ValueError("Missing or invalid accounting status")
        drift = data.get("drift")
        if drift is not None and (not isinstance(drift, dict)
                                  or type(drift.get("success")) is not bool
                                  or not isinstance(drift.get("status"), str)):
            raise ValueError("Malformed drift status")
        inference = data.get("inference")
        if inference is not None and (not isinstance(inference, dict)
                                      or type(inference.get("verified")) is not bool):
            raise ValueError("Malformed inference status")
    except (OSError, UnicodeError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail="Invalid run summary evidence") from exc

    return {
        "run_id": run_id,
        "success": data["success"],
        "accounting_complete": data["accounting_complete"],
        "inference": inference,
        "drift": drift,
        "errors": data.get("errors", []),
        "summary": data,
    }
