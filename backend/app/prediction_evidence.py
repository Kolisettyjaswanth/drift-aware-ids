"""Verified, size-bounded prediction pagination for completed replay runs."""
import json
from pathlib import Path

from fastapi import HTTPException
from src.streaming.verify_inference import verify_inference
from backend.app.run_evidence import read_run_summary

MAX_EVIDENCE_BYTES = 16 * 1024 * 1024
MAX_BATCH_FILES = 128
MAX_LINE_BYTES = 64 * 1024


def read_predictions(root: Path, run_id: str, offset: int, limit: int) -> dict:
    detail = read_run_summary(root, run_id)
    if not (detail["success"] is True and detail["accounting_complete"] is True):
        raise HTTPException(status_code=409, detail="Run is not successfully complete")
    declared = detail["inference"]
    if not isinstance(declared, dict) or declared.get("verified") is not True:
        raise HTTPException(status_code=409, detail="Verified inference unavailable")

    directory = root / run_id
    transport_dir = directory / "batches"
    prediction_dir = directory / "predictions"
    if any(p.is_symlink() or not p.is_dir() for p in (transport_dir, prediction_dir)):
        raise HTTPException(status_code=422, detail="Invalid inference directories")

    transport = sorted(transport_dir.glob("batch-*.jsonl"))
    predictions = sorted(prediction_dir.glob("batch-*.jsonl"))
    if (not transport or len(transport) > MAX_BATCH_FILES
            or len(predictions) > MAX_BATCH_FILES
            or {p.name for p in transport} != {p.name for p in predictions}):
        raise HTTPException(status_code=422, detail="Inference batch mismatch")

    total_bytes = 0
    for path in transport + predictions:
        if path.is_symlink() or not path.is_file():
            raise HTTPException(status_code=422, detail="Unsafe inference evidence")
        total_bytes += path.stat().st_size
        if total_bytes > MAX_EVIDENCE_BYTES:
            raise HTTPException(status_code=413, detail="Evidence exceeds API verification budget")

    try:
        verified = verify_inference(directory)
        if verified != declared:
            raise ValueError("Stored inference verification differs from evidence")
        selected = []
        index = 0
        for path in predictions:
            with path.open("rb") as stream:
                for line in stream:
                    if len(line) > MAX_LINE_BYTES:
                        raise ValueError("Prediction line exceeds limit")
                    if offset <= index < offset + limit:
                        selected.append(json.loads(line))
                    index += 1
        if index != verified["predictions"]:
            raise ValueError("Prediction count differs from verification")
    except (OSError, UnicodeError, ValueError, KeyError, TypeError, IndexError) as exc:
        raise HTTPException(status_code=422, detail="Invalid inference evidence") from exc

    return {
        "run_id": run_id, "verified": True, "total": index,
        "offset": offset, "limit": limit, "predictions": selected,
    }
