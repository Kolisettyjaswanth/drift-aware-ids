
from pathlib import Path
import json

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

DATA_PATH = Path(
    "data/raw/cicids2017/MachineLearningCVE/"
    "Monday-WorkingHours.pcap_ISCX.csv"
)

OUTPUT_PATH = Path("models/drift_benign_control.json")

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

REFERENCE_SIZE = 10000
WINDOW_SIZE = 1000
NUM_WINDOWS = 20
SEED = 42

ALPHA = 0.05
MIN_KS_STATISTIC = 0.10


def main():
    rng = np.random.default_rng(SEED)

    df = pd.read_csv(
        DATA_PATH,
        usecols=lambda col: col.strip() in FEATURES + ["Label"],
        low_memory=False,
        encoding="utf-8-sig",
    )

    df.columns = df.columns.str.strip()
    df["Label"] = df["Label"].astype(str).str.strip()

    df = df[df["Label"] == "BENIGN"].copy()

    required_rows = REFERENCE_SIZE + WINDOW_SIZE * NUM_WINDOWS

    if len(df) < required_rows:
        raise RuntimeError("Not enough benign records")

    # Disjoint random samples: no record appears in
    # both reference and evaluation windows.
    indices = rng.choice(
        len(df),
        size=required_rows,
        replace=False,
    )

    reference = df.iloc[indices[:REFERENCE_SIZE]]
    evaluation = df.iloc[indices[REFERENCE_SIZE:]]

    corrected_alpha = ALPHA / len(FEATURES)
    results = []

    for window_id in range(NUM_WINDOWS):
        start = window_id * WINDOW_SIZE
        end = start + WINDOW_SIZE
        window = evaluation.iloc[start:end]

        drifted_features = []

        for feature in FEATURES:
            ref_values = pd.to_numeric(
                reference[feature],
                errors="coerce",
            ).replace(
                [np.inf, -np.inf], np.nan
            ).dropna()

            window_values = pd.to_numeric(
                window[feature],
                errors="coerce",
            ).replace(
                [np.inf, -np.inf], np.nan
            ).dropna()

            if len(ref_values) < 100 or len(window_values) < 100:
                continue

            statistic, p_value = ks_2samp(
                ref_values,
                window_values,
            )

            if (
                p_value < corrected_alpha
                and statistic >= MIN_KS_STATISTIC
            ):
                drifted_features.append(feature)

        results.append({
            "window": window_id + 1,
            "drifted_feature_count": len(drifted_features),
            "drifted_features": drifted_features,
        })

        print(
            f"Window {window_id + 1}: "
            f"{len(drifted_features)}/{len(FEATURES)} "
            "features drifted"
        )

    flagged = sum(
        result["drifted_feature_count"] > 0
        for result in results
    )

    report = {
        "experiment": "same-capture benign control",
        "source_capture": DATA_PATH.name,
        "reference_size": REFERENCE_SIZE,
        "window_size": WINDOW_SIZE,
        "number_of_windows": NUM_WINDOWS,
        "flagged_windows": flagged,
        "window_flag_rate": flagged / NUM_WINDOWS,
        "alpha": ALPHA,
        "corrected_alpha": corrected_alpha,
        "minimum_ks_statistic": MIN_KS_STATISTIC,
        "windows": results,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print("\n=== BENIGN CONTROL SUMMARY ===")
    print("Windows evaluated:", NUM_WINDOWS)
    print("Windows flagged:", flagged)
    print(
        "False-alarm window rate:",
        round(flagged / NUM_WINDOWS * 100, 2),
        "%",
    )
    print("Report saved:", OUTPUT_PATH)


if __name__ == "__main__":
    main()
