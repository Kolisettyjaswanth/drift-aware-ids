
from pathlib import Path
import json

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

INPUT = Path("data/processed/ml/development_sample.csv")
AUDIT = Path("data/processed/dataset_audit.json")
OUTPUT = Path("data/processed/ml")

SEED = 42

def main():
    df = pd.read_csv(INPUT, low_memory=False)
    audit = json.loads(AUDIT.read_text())

    # Standardize column names.
    df.columns = df.columns.str.strip()

    # Keep original attack labels.
    df["Label"] = df["Label"].astype(str).str.strip()

    # Create binary classification target.
    df["Target"] = (df["Label"] != "BENIGN").astype(int)

    # Remove constant features found during the audit.
    constant_columns = audit["constant_columns"]
    df = df.drop(columns=constant_columns)

    # Separate input features and labels.
    X = df.drop(columns=["Label", "Target"])
    y = df["Target"]

    # Remove exact duplicate feature/target combinations
    # before splitting to reduce direct train/test overlap.
    combined = X.copy()
    combined["Target"] = y

    original_rows = len(combined)
    combined = combined.drop_duplicates()

    duplicate_rows = original_rows - len(combined)

    X = combined.drop(columns=["Target"])
    y = combined["Target"]

    # Ensure numeric feature types.
    X = X.apply(pd.to_numeric, errors="coerce")
    X = X.replace([np.inf, -np.inf], np.nan)

    # Stratified split preserves binary class proportions.
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        random_state=SEED,
        stratify=y,
    )

    OUTPUT.mkdir(parents=True, exist_ok=True)

    train = X_train.copy()
    train["Target"] = y_train

    test = X_test.copy()
    test["Target"] = y_test

    train.to_csv(OUTPUT / "train.csv", index=False)
    test.to_csv(OUTPUT / "test.csv", index=False)

    metadata = {
        "seed": SEED,
        "original_rows": original_rows,
        "duplicates_removed": duplicate_rows,
        "rows_after_deduplication": len(combined),
        "training_rows": len(train),
        "testing_rows": len(test),
        "feature_count": X.shape[1],
        "train_class_counts": {
            str(k): int(v)
            for k, v in y_train.value_counts().items()
        },
        "test_class_counts": {
            str(k): int(v)
            for k, v in y_test.value_counts().items()
        },
        "split_type": "stratified random development split",
        "evaluation_warning": (
            "This split is a development benchmark, "
            "not a leakage-free temporal or deployment evaluation."
        ),
    }

    (OUTPUT / "baseline_metadata.json").write_text(
        json.dumps(metadata, indent=2)
    )

    print("\n=== BASELINE DATA PREPARATION ===")
    for key, value in metadata.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
