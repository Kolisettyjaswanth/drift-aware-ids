
from pathlib import Path
import json

import numpy as np
import pandas as pd

RAW_DIR = Path("data/raw/cicids2017")
OUTPUT_DIR = Path("data/processed")
OUTPUT_FILE = OUTPUT_DIR / "capture_aware_cicids2017.csv"
REPORT_FILE = OUTPUT_DIR / "capture_dataset_report.json"

CHUNK_SIZE = 50_000


def main():
    files = sorted(RAW_DIR.rglob("*.csv"))

    if len(files) != 8:
        raise RuntimeError(
            f"Expected 8 CSV files, found {len(files)}"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    expected_columns = None
    first_write = True
    total_rows = 0
    file_counts = {}

    for file_path in files:
        file_rows = 0

        print(f"Processing: {file_path.name}", flush=True)

        for chunk in pd.read_csv(
            file_path,
            chunksize=CHUNK_SIZE,
            low_memory=False,
            encoding="utf-8-sig",
        ):
            chunk.columns = chunk.columns.str.strip()

            if expected_columns is None:
                expected_columns = chunk.columns.tolist()
            elif chunk.columns.tolist() != expected_columns:
                raise ValueError(
                    f"Schema mismatch: {file_path.name}"
                )

            chunk["Label"] = (
                chunk["Label"]
                .astype("string")
                .str.strip()
            )

            invalid_labels = (
                chunk["Label"].isna()
                | chunk["Label"].eq("")
            )

            chunk = chunk.loc[~invalid_labels].copy()

            feature_columns = [
                col for col in chunk.columns
                if col != "Label"
            ]

            for column in feature_columns:
                chunk[column] = (
                    pd.to_numeric(
                        chunk[column],
                        errors="coerce",
                    )
                    .replace([np.inf, -np.inf], np.nan)
                )

            # Metadata for splitting, never an ML feature.
            chunk["SourceFile"] = file_path.name

            chunk.to_csv(
                OUTPUT_FILE,
                mode="w" if first_write else "a",
                index=False,
                header=first_write,
                na_rep="",
            )

            first_write = False
            file_rows += len(chunk)
            total_rows += len(chunk)

        file_counts[file_path.name] = file_rows
        print(f"Completed: {file_rows:,} rows", flush=True)

    report = {
        "total_rows": total_rows,
        "total_columns": len(expected_columns) + 1,
        "source_files": file_counts,
        "source_file_column": "SourceFile",
        "source_file_is_model_feature": False,
        "output_file": str(OUTPUT_FILE),
    }

    REPORT_FILE.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print("\n=== CAPTURE DATASET SUMMARY ===")
    print("Rows:", total_rows)
    print("Columns:", report["total_columns"])
    print("Source files:", len(file_counts))
    print("Output:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
