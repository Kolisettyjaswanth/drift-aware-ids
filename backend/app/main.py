"""Read-only API for bounded Spark replay evidence."""
from pathlib import Path
import os
import re

from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="Drift-Aware IDS Evidence API", version="0.1.0")
RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


def evidence_root() -> Path:
    return Path(os.environ.get("IDS_EVIDENCE_ROOT", "data/processed/spark")).resolve()


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "ids-evidence-api"}


@app.get("/api/runs")
def list_runs(limit: int = Query(default=20, ge=1, le=100)):
    root = evidence_root()
    if not root.is_dir():
        raise HTTPException(status_code=503, detail="Evidence directory unavailable")

    runs = []
    for entry in sorted(root.iterdir(), key=lambda p: p.name, reverse=True):
        if len(runs) >= limit:
            break
        if not RUN_ID.fullmatch(entry.name) or entry.is_symlink() or not entry.is_dir():
            continue
        summary = entry / "summary.json"
        if summary.is_symlink() or not summary.is_file():
            continue
        runs.append({"run_id": entry.name, "summary_present": True})
    return {"runs": runs, "count": len(runs), "limit": limit}

@app.get("/api/runs/{run_id}")
def get_run(run_id: str):
    from backend.app.run_evidence import read_run_summary
    return read_run_summary(evidence_root(), run_id)
