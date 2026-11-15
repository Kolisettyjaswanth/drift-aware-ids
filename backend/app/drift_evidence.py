"""Read-only authoritative drift snapshot evidence, without event membership."""
import json
import re
from pathlib import Path

from fastapi import HTTPException
from backend.app.run_evidence import read_run_summary

SHA256 = re.compile(r"^[0-9a-f]{64}$")
MAX_REPORT_BYTES = 8 * 1024 * 1024
MAX_WINDOWS = 10


def read_drift(root: Path, run_id: str) -> dict:
    detail = read_run_summary(root, run_id)
    drift = detail["drift"]
    if drift is None:
        return {"run_id": run_id, "status": "disabled", "success": False,
                "evaluation_complete": False, "evaluated_windows": 0,
                "snapshot_id": None, "windows": [], "errors": []}

    status = drift["status"]
    snapshot_id = drift.get("snapshot_id")
    base = {"run_id": run_id, "status": status, "success": drift["success"],
            "evaluation_complete": drift.get("evaluation_complete") is True,
            "evaluated_windows": drift.get("evaluated_windows", 0),
            "snapshot_id": snapshot_id, "reference_sha256": drift.get("reference_sha256"),
            "windows": drift.get("windows", []), "errors": drift.get("errors", [])}

    if not isinstance(base["errors"], list) or not isinstance(base["windows"], list):
        raise HTTPException(status_code=422, detail="Malformed drift summary")
    if not isinstance(base["evaluated_windows"], int) or type(base["evaluated_windows"]) is bool:
        raise HTTPException(status_code=422, detail="Malformed drift window count")

    if not SHA256.fullmatch(snapshot_id or ""):
        if drift["success"] is True or base["evaluation_complete"]:
            raise HTTPException(status_code=422, detail="Missing authoritative snapshot")
        return {**base, "windows": _compact_windows(base["windows"])}

    directory = root / run_id / "drift" / "evaluations"
    path = directory / (snapshot_id + ".json")
    if directory.is_symlink() or path.is_symlink() or not path.is_file():
        raise HTTPException(status_code=422, detail="Drift snapshot unavailable")
    try:
        if path.stat().st_size > MAX_REPORT_BYTES:
            raise ValueError("Oversized drift report")
        with path.open("rb") as stream:
            raw = stream.read(MAX_REPORT_BYTES + 1)
        if len(raw) > MAX_REPORT_BYTES:
            raise ValueError("Oversized drift report")
        report = json.loads(raw)
        if not isinstance(report, dict):
            raise ValueError("Malformed drift report")
        for field in ("snapshot_id", "status", "success", "evaluation_complete",
                      "evaluated_windows"):
            expected = drift.get(field)
            if field in ("success", "evaluation_complete"):
                if type(report.get(field)) is not bool or report[field] is not expected:
                    raise ValueError("Drift status disagreement")
            elif report.get(field) != expected:
                raise ValueError("Drift status disagreement")
        if not isinstance(report.get("reference"), dict) or (
                report["reference"].get("sha256") != drift.get("reference_sha256")):
            raise ValueError("Drift reference mismatch")
        windows = report.get("windows")
        if not isinstance(windows, list) or len(windows) > MAX_WINDOWS:
            raise ValueError("Malformed drift windows")
        compact = _compact_windows(windows)
        if sum(w["status"] == "evaluated" for w in compact) != report["evaluated_windows"]:
            raise ValueError("Drift evaluation count mismatch")
        summary_windows = _compact_windows(base["windows"])
        if compact != summary_windows:
            raise ValueError("Drift window summary mismatch")
        return {**base, "windows": compact}
    except (OSError, ValueError, TypeError, UnicodeError, KeyError) as exc:
        raise HTTPException(status_code=422, detail="Invalid drift snapshot evidence") from exc


def _compact_windows(windows):
    if not isinstance(windows, list) or len(windows) > MAX_WINDOWS:
        raise ValueError("Malformed drift windows")
    compact = []
    for window in windows:
        if not isinstance(window, dict):
            raise ValueError("Malformed drift window")
        status = window.get("status")
        alert = window.get("alert")
        if not isinstance(status, str) or (
                alert is not None and type(alert) is not bool):
            raise ValueError("Malformed drift alert")
        if status != "evaluated" and alert is not None:
            raise ValueError("Unevaluated window has boolean alert")
        if status == "evaluated" and type(alert) is not bool:
            raise ValueError("Evaluated window lacks boolean alert")
        item = {key: window.get(key) for key in (
            "window_index", "status", "records", "alert", "source_capture")}
        if type(item["window_index"]) is not int or type(item["records"]) is not int:
            raise ValueError("Malformed drift window metadata")
        if "ks_results" in window:
            item["ks_results"] = window["ks_results"]
        compact.append(item)
    return compact
