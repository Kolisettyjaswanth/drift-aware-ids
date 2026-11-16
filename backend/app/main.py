"""Read-only API for bounded Spark replay evidence."""
from pathlib import Path
from datetime import datetime
import json
import os
import re

from fastapi import FastAPI, HTTPException, Query
from backend.app.run_evidence import MAX_SUMMARY_BYTES

app = FastAPI(title="Drift-Aware IDS Evidence API", version="0.1.0")
RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


def evidence_root() -> Path:
    return Path(os.environ.get("IDS_EVIDENCE_ROOT", "data/processed/spark")).resolve()


def run_timestamp(directory: Path, summary: Path) -> float:
    """Creation time is stable across updates; legacy evidence uses file times."""
    try:
        with summary.open('rb') as stream:
            raw = stream.read(MAX_SUMMARY_BYTES + 1)
        data = json.loads(raw) if len(raw) <= MAX_SUMMARY_BYTES else None
        value = data.get('created_at') if isinstance(data, dict) else None
        if isinstance(value, str):
            stamp = datetime.fromisoformat(value)
            if stamp.tzinfo is not None and stamp.utcoffset() is not None:
                return stamp.timestamp()
    except (OSError, ValueError, OverflowError, RecursionError):
        pass
    config = directory / 'config.json'
    if not config.is_symlink() and config.is_file():
        return config.stat().st_mtime
    return summary.stat().st_mtime


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "ids-evidence-api"}


@app.get("/api/runs")
def list_runs(limit: int = Query(default=20, ge=1, le=100)):
    root = evidence_root()
    if not root.is_dir():
        raise HTTPException(status_code=503, detail="Evidence directory unavailable")

    candidates = []
    for entry in root.iterdir():
        if not RUN_ID.fullmatch(entry.name) or entry.is_symlink() or not entry.is_dir():
            continue
        summary = entry / "summary.json"
        if summary.is_symlink() or not summary.is_file():
            continue
        try:
            candidates.append((run_timestamp(entry, summary), entry.name))
        except OSError:
            continue  # Evidence disappeared while scanning; do not fail other runs.
    runs = [{"run_id": name, "summary_present": True}
            for _, name in sorted(candidates, reverse=True)[:limit]]
    return {"runs": runs, "count": len(runs), "limit": limit}

@app.get("/api/runs/{run_id}")
def get_run(run_id: str):
    from backend.app.run_evidence import read_run_summary
    return read_run_summary(evidence_root(), run_id)

@app.get("/api/runs/{run_id}/predictions")
def get_predictions(
    run_id: str,
    offset: int = Query(default=0, ge=0, le=1000000),
    limit: int = Query(default=50, ge=1, le=100),
):
    from backend.app.prediction_evidence import read_predictions
    return read_predictions(evidence_root(), run_id, offset, limit)

@app.get("/api/runs/{run_id}/drift")
def get_drift(run_id: str):
    from backend.app.drift_evidence import read_drift
    return read_drift(evidence_root(), run_id)

@app.get("/api/runs/{run_id}/prediction-counts")
def get_prediction_counts(run_id: str):
    from backend.app.prediction_evidence import read_prediction_counts
    return read_prediction_counts(evidence_root(), run_id)
