"""Validate inputs and record provenance for the existing drift/error analysis.

This preflight does not retrain models, alter thresholds or modify input files.
"""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.drift.analyze_drift_vs_errors import FEATURES, WINDOW_SIZE


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect(reference_path, predictions_path):
    for path in (reference_path, predictions_path):
        if not path.is_file():
            raise ValueError(f"Missing input: {path}")
    reference = pd.read_csv(reference_path)
    predictions = pd.read_csv(predictions_path)
    for name, frame in (("reference", reference), ("predictions", predictions)):
        if frame.empty:
            raise ValueError(f"{name} is empty")
        if frame.columns.duplicated().any():
            raise ValueError(f"{name} has duplicate column names")
        missing = sorted(set(FEATURES) - set(frame.columns))
        if missing:
            raise ValueError(f"{name} missing features: {missing}")
        numeric = frame[FEATURES].apply(pd.to_numeric, errors="coerce")
        if not np.isfinite(numeric.to_numpy(dtype=float)).all():
            raise ValueError(f"{name} has missing or nonfinite monitored features")
    required = {"Actual", "Predicted", "Correct", "Label", "SourceFile"}
    missing = sorted(required - set(predictions.columns))
    if missing:
        raise ValueError(f"predictions missing columns: {missing}")
    for name in ("Actual", "Predicted", "Correct"):
        if not predictions[name].isin([0, 1]).all():
            raise ValueError(f"{name} must contain only 0 and 1")
    expected = predictions["Actual"].eq(predictions["Predicted"]).astype(int)
    if not predictions["Correct"].eq(expected).all():
        raise ValueError("Correct disagrees with Actual and Predicted")
    if predictions["Label"].isna().any():
        raise ValueError("Missing labels")
    expected_actual = predictions["Label"].astype(str).str.strip().ne("BENIGN").astype(int)
    if not predictions["Actual"].eq(expected_actual).all():
        raise ValueError("Actual disagrees with Label")
    if predictions["SourceFile"].isna().any() or predictions["SourceFile"].nunique() != 1:
        raise ValueError("Predictions must represent exactly one named capture")
    windows, trailing = divmod(len(predictions), WINDOW_SIZE)
    if windows < 4:
        raise ValueError("At least four complete windows required")
    return {
        "reference": {"path": str(reference_path), "sha256": sha256(reference_path), "rows": len(reference)},
        "predictions": {"path": str(predictions_path), "sha256": sha256(predictions_path),
                        "rows": len(predictions), "capture": str(predictions["SourceFile"].iloc[0])},
        "window_size": WINDOW_SIZE,
        "complete_windows": windows,
        "excluded_trailing_records": trailing,
        "limitations": [
            "Original source row IDs are absent from the prediction export",
            "Export order is not independently verified as event-time order",
            "Reference/train separation requires separate dataset lineage evidence",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, default=Path("data/processed/drift/reference.csv"))
    parser.add_argument("--predictions", type=Path, default=Path("data/processed/drift/heldout_portscan_predictions.csv"))
    parser.add_argument("--output", type=Path, default=Path("models/increment4_preflight.json"))
    args = parser.parse_args()
    report = inspect(args.reference, args.predictions)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
