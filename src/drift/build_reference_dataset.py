
import json
from pathlib import Path

import numpy as np
import pandas as pd

INPUT = Path("data/processed/capture_aware_cicids2017.csv")
OUTPUT = Path("data/processed/drift")

REFERENCE_CAPTURES = [
    "Monday-WorkingHours.pcap_ISCX.csv",
    "Tuesday-WorkingHours.pcap_ISCX.csv",
    "Wednesday-workingHours.pcap_ISCX.csv",
    "Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv",
]

CALIBRATION_CAPTURES = [
    "Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv",
    "Friday-WorkingHours-Morning.pcap_ISCX.csv",
]

EVALUATION_CAPTURES = [
    "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv",
    "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv",
]

FEATURES = [
    "Average Packet Size",
    "Bwd Packet Length Std",
    "Packet Length Variance",
    "Max Packet Length",
    "Bwd Packet Length Mean",
    "Packet Length Mean",
    "Destination Port",
    "Fwd Packet Length Max",
    "Total Length of Bwd Packets",
    "Packet Length Std",
]

SAMPLES_PER_CAPTURE = 10000
RANDOM_STATE = 42


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)

    columns = FEATURES + ["Label", "SourceFile"]

    df = pd.read_csv(
        INPUT,
        usecols=columns,
        low_memory=False,
    )

    df["Label"] = df["Label"].astype(str).str.strip()
    df["SourceFile"] = df["SourceFile"].astype(str).str.strip()

    all_captures = (
        REFERENCE_CAPTURES
        + CALIBRATION_CAPTURES
        + EVALUATION_CAPTURES
    )

    assert len(all_captures) == len(set(all_captures)), (
        "Capture overlap detected!"
    )

    actual_captures = set(df["SourceFile"].unique())
    assert set(all_captures) == actual_captures, (
        f"Capture mismatch: {set(all_captures) ^ actual_captures}"
    )

    benign = df[df["Label"] == "BENIGN"].copy()

    for feature in FEATURES:
        benign[feature] = pd.to_numeric(
            benign[feature],
            errors="coerce",
        ).replace([np.inf, -np.inf], np.nan)

    benign = benign.dropna(subset=FEATURES)

    report = {
        "random_state": RANDOM_STATE,
        "samples_per_capture": SAMPLES_PER_CAPTURE,
        "groups": {},
    }

    groups = {
        "reference": REFERENCE_CAPTURES,
        "calibration": CALIBRATION_CAPTURES,
        "evaluation_benign": EVALUATION_CAPTURES,
    }

    for group_name, captures in groups.items():
        samples = []
        capture_counts = {}

        for capture in captures:
            subset = benign[benign["SourceFile"] == capture]

            assert len(subset) >= SAMPLES_PER_CAPTURE, (
                f"Insufficient benign records in {capture}"
            )

            sample = subset.sample(
                n=SAMPLES_PER_CAPTURE,
                random_state=RANDOM_STATE,
            )

            samples.append(sample)
            capture_counts[capture] = len(sample)

        result = pd.concat(samples, ignore_index=True)

        output_path = OUTPUT / f"{group_name}.csv"
        result.to_csv(output_path, index=False)

        report["groups"][group_name] = {
            "captures": capture_counts,
            "total_rows": len(result),
            "output": str(output_path),
        }

        print(f"{group_name}: {len(result):,} rows")

    report_path = OUTPUT / "split_manifest.json"
    report_path.write_text(json.dumps(report, indent=2))

    print("\nSplit manifest:", report_path)
    print("Dataset preparation completed.")


if __name__ == "__main__":
    main()
