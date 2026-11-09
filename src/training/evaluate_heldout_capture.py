
from pathlib import Path
from collections import Counter
import json

import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)

DATA = Path("data/processed/capture_aware_cicids2017.csv")
AUDIT = Path("data/processed/dataset_audit.json")
OUTPUT = Path("models/heldout_portscan_metrics.json")

HOLDOUT = (
    "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv"
)

CHUNK_SIZE = 50_000
MAX_TRAIN_ROWS = 300_000
SEED = 42


def main():
    audit = json.loads(AUDIT.read_text())
    excluded = set(audit["constant_columns"])

    rng = np.random.default_rng(SEED)
    train_parts = []
    test_parts = []

    # Reservoir-like bounded sampling of training rows.
    # Assign a random key to each row and retain the
    # smallest keys, approximating a uniform sample.
    for chunk in pd.read_csv(
        DATA,
        chunksize=CHUNK_SIZE,
        low_memory=False,
    ):
        chunk.columns = chunk.columns.str.strip()

        is_test = chunk["SourceFile"].eq(HOLDOUT)

        test_chunk = chunk.loc[is_test].copy()
        train_chunk = chunk.loc[~is_test].copy()

        if not test_chunk.empty:
            test_parts.append(test_chunk)

        if not train_chunk.empty:
            train_chunk["_sample_key"] = rng.random(
                len(train_chunk)
            )
            train_parts.append(train_chunk)

        print("Processed chunk", flush=True)

    train = pd.concat(train_parts, ignore_index=True)
    test = pd.concat(test_parts, ignore_index=True)

    if len(train) > MAX_TRAIN_ROWS:
        train = train.nsmallest(
            MAX_TRAIN_ROWS,
            "_sample_key",
        ).copy()

    train = train.drop(columns=["_sample_key"])

    train_labels = sorted(train["Label"].astype(str).unique())
    test_labels = sorted(test["Label"].astype(str).unique())

    train["Target"] = (
        train["Label"].astype(str).str.strip() != "BENIGN"
    ).astype(int)

    test["Target"] = (
        test["Label"].astype(str).str.strip() != "BENIGN"
    ).astype(int)

    feature_columns = [
        col for col in train.columns
        if col not in excluded
        and col not in {"Label", "Target", "SourceFile"}
    ]

    X_train = train[feature_columns]
    y_train = train["Target"]

    X_test = test[feature_columns]
    y_test = test["Target"]

    pipeline = Pipeline([
        (
            "imputer",
            SimpleImputer(
                strategy="median",
                keep_empty_features=True,
            ),
        ),
        (
            "classifier",
            RandomForestClassifier(
                n_estimators=100,
                max_depth=20,
                min_samples_leaf=2,
                class_weight="balanced_subsample",
                random_state=SEED,
                n_jobs=2,
            ),
        ),
    ])

    print("Training held-out evaluation model...")
    pipeline.fit(X_train, y_train)

    print("Evaluating held-out capture...")
    predictions = pipeline.predict(X_test)

    results = {
        "holdout_capture": HOLDOUT,
        "training_rows": len(train),
        "testing_rows": len(test),
        "training_labels": train_labels,
        "testing_labels": test_labels,
        "unseen_test_labels": sorted(
            set(test_labels) - set(train_labels)
        ),
        "accuracy": float(
            accuracy_score(y_test, predictions)
        ),
        "precision": float(
            precision_score(
                y_test, predictions, zero_division=0
            )
        ),
        "recall": float(
            recall_score(
                y_test, predictions, zero_division=0
            )
        ),
        "f1_score": float(
            f1_score(
                y_test, predictions, zero_division=0
            )
        ),
        "confusion_matrix": confusion_matrix(
            y_test, predictions, labels=[0, 1]
        ).tolist(),
        "evaluation_type": "held-out capture",
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(results, indent=2))

    print("\n=== HELD-OUT CAPTURE EVALUATION ===")
    print("Training rows:", len(train))
    print("Testing rows:", len(test))
    print("Unseen labels:", results["unseen_test_labels"])
    print("Accuracy:", round(results["accuracy"] * 100, 4), "%")
    print("Precision:", round(results["precision"] * 100, 4), "%")
    print("Recall:", round(results["recall"] * 100, 4), "%")
    print("F1:", round(results["f1_score"] * 100, 4), "%")
    print("Confusion matrix:", results["confusion_matrix"])


if __name__ == "__main__":
    main()
