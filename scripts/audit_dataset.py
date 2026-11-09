
from pathlib import Path
from collections import Counter
import json

import numpy as np
import pandas as pd

INPUT = Path("data/processed/cleaned_cicids2017.csv")
OUTPUT = Path("data/processed/dataset_audit.json")
CHUNK_SIZE = 50_000


def main():
    if not INPUT.exists():
        raise FileNotFoundError(INPUT)

    missing = Counter()
    labels = Counter()
    minimum = {}
    maximum = {}
    total_rows = 0
    columns = None

    for index, chunk in enumerate(
        pd.read_csv(INPUT, chunksize=CHUNK_SIZE, low_memory=False),
        start=1,
    ):
        if columns is None:
            columns = chunk.columns.tolist()

        total_rows += len(chunk)

        labels.update(
            chunk["Label"].astype("string").fillna("<MISSING>")
        )

        missing.update(
            chunk.isna().sum().astype(int).to_dict()
        )

        numeric = chunk.drop(columns=["Label"])

        for col in numeric.columns:
            values = pd.to_numeric(numeric[col], errors="coerce")
            valid = values.dropna()

            if valid.empty:
                continue

            low = float(valid.min())
            high = float(valid.max())

            minimum[col] = min(minimum.get(col, low), low)
            maximum[col] = max(maximum.get(col, high), high)

        print(
            f"Processed chunk {index}: {total_rows:,} rows",
            flush=True,
        )

    constant_columns = [
        col for col in minimum
        if minimum[col] == maximum[col]
    ]

    all_missing_columns = [
        col for col in columns
        if missing[col] == total_rows
    ]

    report = {
        "total_rows": total_rows,
        "total_columns": len(columns),
        "label_counts": dict(labels),
        "missing_values": {
            col: count
            for col, count in missing.items()
            if count > 0
        },
        "constant_columns": constant_columns,
        "all_missing_columns": all_missing_columns,
        "feature_minimum": minimum,
        "feature_maximum": maximum,
        "duplicate_rows_checked": False,
        "duplicate_rows_note": (
            "Exact duplicates not yet measured. "
            "Requires a separate memory-aware analysis."
        ),
    }

    OUTPUT.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print("\n=== DATASET AUDIT ===")
    print("Rows:", total_rows)
    print("Columns:", len(columns))
    print("Classes:", len(labels))
    print("Columns with missing values:", sum(
        count > 0 for count in missing.values()
    ))
    print("Constant columns:", constant_columns)
    print("Entirely missing columns:", all_missing_columns)
    print("Exact duplicates: NOT YET CHECKED")
    print("Report saved:", OUTPUT)


if __name__ == "__main__":
    main()
