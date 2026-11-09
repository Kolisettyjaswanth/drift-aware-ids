
from pathlib import Path
import json

import numpy as np
import pandas as pd

RAW_DIR = Path("data/raw/cicids2017")
OUTPUT_DIR = Path("data/processed")
OUTPUT_FILE = OUTPUT_DIR / "cleaned_cicids2017.csv"
REPORT_FILE = OUTPUT_DIR / "cleaning_report.json"

CHUNK_SIZE = 50_000


def main():
    files = sorted(RAW_DIR.rglob("*.csv"))

    if len(files) != 8:
        raise RuntimeError(
            f"Expected 8 CSV files, found {len(files)}"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_input = 0
    total_output = 0
    missing_labels = 0
    infinite_values = 0
    invalid_numeric_values = 0

    first_write = True
    expected_columns = None

    for file_path in files:
        print(f"Processing: {file_path.name}", flush=True)

        for chunk in pd.read_csv(
            file_path,
            chunksize=CHUNK_SIZE,
            low_memory=False,
            encoding="utf-8-sig",
        ):
            total_input += len(chunk)

            chunk.columns = chunk.columns.str.strip()

            if expected_columns is None:
                expected_columns = chunk.columns.tolist()
            elif chunk.columns.tolist() != expected_columns:
                raise ValueError(
                    f"Column mismatch in {file_path.name}"
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

            missing_labels += int(invalid_labels.sum())

            chunk = chunk.loc[~invalid_labels].copy()

            feature_columns = [
                col for col in chunk.columns
                if col != "Label"
            ]

            for column in feature_columns:
                original = chunk[column]

                converted = pd.to_numeric(
                    original,
                    errors="coerce",
                )

                invalid_numeric_values += int(
                    (
                        original.notna()
                        & converted.isna()
                    ).sum()
                )

                infinite_values += int(
                    np.isinf(converted.to_numpy()).sum()
                )

                chunk[column] = converted.replace(
                    [np.inf, -np.inf],
                    np.nan,
                )

            chunk.to_csv(
                OUTPUT_FILE,
                mode="w" if first_write else "a",
                index=False,
                header=first_write,
                na_rep="",
            )

            first_write = False
            total_output += len(chunk)

        print("Completed.", flush=True)

    report = {
        "input_rows": total_input,
        "output_rows": total_output,
        "removed_missing_label_rows": missing_labels,
        "infinite_feature_values_replaced": infinite_values,
        "invalid_numeric_values": invalid_numeric_values,
        "output_file": str(OUTPUT_FILE),
    }

    REPORT_FILE.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print("\n=== CLEANING SUMMARY ===")
    for key, value in report.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
