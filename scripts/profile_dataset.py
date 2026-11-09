
from pathlib import Path
from collections import Counter
import json

import numpy as np
import pandas as pd

DATA_DIR = Path("data/raw/cicids2017")
REPORT_PATH = Path("data/processed/dataset_profile.json")
CHUNK_SIZE = 50_000


def profile_file(file_path):
    label_counts = Counter()
    missing_values = Counter()
    infinite_values = Counter()
    total_rows = 0

    columns = None

    for chunk in pd.read_csv(
        file_path,
        chunksize=CHUNK_SIZE,
        low_memory=False,
        encoding="utf-8-sig",
    ):
        chunk.columns = chunk.columns.str.strip()

        if columns is None:
            columns = chunk.columns.tolist()

        labels = (
            chunk["Label"]
            .astype("string")
            .str.strip()
            .fillna("<MISSING>")
        )

        label_counts.update(labels.tolist())
        total_rows += len(chunk)

        for column in chunk.columns:
            missing_values[column] += int(
                chunk[column].isna().sum()
            )

        numeric = chunk.select_dtypes(
            include=[np.number]
        )

        inf_counts = np.isinf(
            numeric.to_numpy(dtype=np.float64)
        ).sum(axis=0)

        for column, count in zip(
            numeric.columns,
            inf_counts,
        ):
            infinite_values[column] += int(count)

    return {
        "file": file_path.name,
        "total_rows": total_rows,
        "columns": columns,
        "label_counts": dict(label_counts),
        "missing_values": {
            k: v for k, v in missing_values.items()
            if v > 0
        },
        "infinite_values": {
            k: v for k, v in infinite_values.items()
            if v > 0
        },
    }


def main():
    files = sorted(DATA_DIR.rglob("*.csv"))

    if len(files) != 8:
        raise RuntimeError(
            f"Expected 8 CSV files, found {len(files)}"
        )

    reports = []
    overall_labels = Counter()
    total_rows = 0

    for index, file_path in enumerate(files, 1):
        print(
            f"[{index}/{len(files)}] "
            f"Profiling {file_path.name}",
            flush=True,
        )

        report = profile_file(file_path)
        reports.append(report)

        overall_labels.update(
            report["label_counts"]
        )

        total_rows += report["total_rows"]

        print(
            f"  Records: {report['total_rows']:,}",
            flush=True,
        )

    # Check whether every file uses the same schema.
    reference_columns = reports[0]["columns"]

    schema_consistent = all(
        report["columns"] == reference_columns
        for report in reports
    )

    result = {
        "total_rows": total_rows,
        "schema_consistent": schema_consistent,
        "overall_label_counts": dict(
            overall_labels
        ),
        "files": reports,
    }

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_PATH.write_text(
        json.dumps(result, indent=2),
        encoding="utf-8",
    )

    print("\n=== DATASET SUMMARY ===")
    print(f"Total records: {total_rows:,}")
    print(f"Schema consistent: {schema_consistent}")

    print("\nAttack distribution:")

    for label, count in overall_labels.most_common():
        percentage = 100 * count / total_rows
        print(
            f"{label}: {count:,} "
            f"({percentage:.2f}%)"
        )

    print(f"\nReport saved: {REPORT_PATH}")


if __name__ == "__main__":
    main()
