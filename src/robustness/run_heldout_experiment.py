"""Offline held-out robustness experiment using one fitted classifier.

Does not overwrite Increment 4 predictions, metrics, or dataset files.
"""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from src.training import evaluate_heldout_capture as heldout
from src.training.sample_capture_data import sample_training_data
from src.robustness.evaluate_perturbations import evaluate_perturbation, save_report

ALLOWED_FEATURES = ("Average Packet Size", "Packet Length Mean", "Packet Length Std")
STRENGTHS = (0.0, 0.05, 0.10)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def prepare_data(train, test, excluded):
    if train["SourceFile"].eq(heldout.HOLDOUT).any():
        raise ValueError("Training sample includes holdout capture")
    if test.empty or not test["SourceFile"].eq(heldout.HOLDOUT).all():
        raise ValueError("Test data must contain only the holdout capture")
    features = [col for col in train.columns
                if col not in excluded and col not in {"Label", "Target", "SourceFile"}]
    if len(features) != len(set(features)) or not set(features).issubset(test.columns):
        raise ValueError("Invalid or missing model features")
    if not set(ALLOWED_FEATURES).issubset(features):
        raise ValueError("Missing approved perturbation features")
    if any(not pd.api.types.is_numeric_dtype(train[col]) for col in features):
        raise ValueError("Non-numeric model feature")
    y_train = train["Label"].astype(str).str.strip().ne("BENIGN").astype(int)
    y_test = test["Label"].astype(str).str.strip().ne("BENIGN").astype(int)
    return features, y_train, y_test


def run(output, max_test_rows=None):
    if max_test_rows is not None and max_test_rows <= 0:
        raise ValueError("max_test_rows must be positive")
    audit = json.loads(heldout.AUDIT.read_text())
    print("Sampling capture-separated training data...", flush=True)
    train = sample_training_data(heldout.HOLDOUT)
    parts = []
    count = 0
    print("Reading held-out capture...", flush=True)
    for chunk in pd.read_csv(heldout.DATA, chunksize=heldout.CHUNK_SIZE, low_memory=False):
        chunk.columns = chunk.columns.str.strip()
        selected = chunk.loc[chunk["SourceFile"].eq(heldout.HOLDOUT)]
        if max_test_rows is not None:
            selected = selected.iloc[:max(0, max_test_rows - count)]
        if not selected.empty:
            parts.append(selected.copy())
            count += len(selected)
        if max_test_rows is not None and count >= max_test_rows:
            break
    if not parts:
        raise ValueError("No held-out test rows")
    test = pd.concat(parts, ignore_index=True)
    features, y_train, y_test = prepare_data(train, test, set(audit["constant_columns"]))
    model = Pipeline([
        ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
        ("classifier", RandomForestClassifier(
            n_estimators=100, max_depth=20, min_samples_leaf=2,
            class_weight="balanced_subsample", random_state=heldout.SEED, n_jobs=2)),
    ])
    print("Fitting classifier once...", flush=True)
    model.fit(train[features], y_train)
    X_test = test[features]
    results = []
    for feature in ALLOWED_FEATURES:
        for strength in STRENGTHS:
            print(f"Evaluating {feature} strength={strength}", flush=True)
            results.append(evaluate_perturbation(model, X_test, y_test, feature, strength))
    report = {
        "experiment": "increment5_offline_heldout_tabular_perturbation",
        "holdout": heldout.HOLDOUT,
        "training_rows": len(train),
        "test_rows": len(test),
        "test_truncated": max_test_rows is not None,
        "feature_order": features,
        "seed": heldout.SEED,
        "dataset_sha256": sha256(heldout.DATA),
        "audit_sha256": sha256(heldout.AUDIT),
        "model": "RandomForestClassifier(n_estimators=100,max_depth=20,min_samples_leaf=2,class_weight=balanced_subsample,random_state=42,n_jobs=2);SimpleImputer(median,keep_empty_features=True)",
        "strengths": list(STRENGTHS),
        "results": results,
        "limitations": [
            "Independent tabular feature edits are not validated network flows",
            "Training sample row identities are not separately persisted",
            "Truncated test runs are diagnostics, not full held-out evaluation",
            "Capture CSV row order is not proven event-time order",
        ],
    }
    save_report(report, output)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("models/increment5_robustness.json"))
    parser.add_argument("--max-test-rows", type=int, default=None,
                        help="Diagnostic only; omit for full held-out capture")
    args = parser.parse_args()
    report = run(args.output, args.max_test_rows)
    print(json.dumps({"output": str(args.output), "training_rows": report["training_rows"],
                      "test_rows": report["test_rows"], "experiments": len(report["results"])}, indent=2))


if __name__ == "__main__":
    main()
