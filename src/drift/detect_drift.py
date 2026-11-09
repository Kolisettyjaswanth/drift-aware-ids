
from pathlib import Path
import json

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

DATA_PATH = Path(
    "data/processed/capture_aware_cicids2017.csv"
)
OUTPUT_PATH = Path("models/drift_portscan_report.json")

HOLDOUT = (
    "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv"
)

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

    reference_parts = []
    holdout_parts = []

    reference_seen = 0
    holdout_seen = 0

    # Uniform reservoir sampling, performed separately
    # for reference and held-out traffic.
    reference = []
    holdout = []

    columns = FEATURES + ["SourceFile"]

    for chunk in pd.read_csv(
        DATA_PATH,
        usecols=columns,
        chunksize=50000,
        low_memory=False,
    ):
        for row in chunk.itertuples(index=False, name=None):
            record = dict(zip(chunk.columns, row))
            source = record.pop("SourceFile")

            if source == HOLDOUT:
                holdout_seen += 1

                if len(holdout) < WINDOW_SIZE * NUM_WINDOWS:
                    holdout.append(record)
                else:
                    j = rng.integers(0, holdout_seen)
                    if j < len(holdout):
                        holdout[j] = record

            else:
                reference_seen += 1

                if len(reference) < REFERENCE_SIZE:
                    reference.append(record)
                else:
                    j = rng.integers(0, reference_seen)
                    if j < len(reference):
                        reference[j] = record

    reference_df = pd.DataFrame(reference)
    holdout_df = pd.DataFrame(holdout)

    print("Reference records:", len(reference_df))
    print("Held-out records:", len(holdout_df))

    if len(reference_df) != REFERENCE_SIZE:
        raise RuntimeError("Insufficient reference records")

    if len(holdout_df) != WINDOW_SIZE * NUM_WINDOWS:
        raise RuntimeError("Insufficient held-out records")

    corrected_alpha = ALPHA / len(FEATURES)

    results = []

    for window_id in range(NUM_WINDOWS):
        start = window_id * WINDOW_SIZE
        end = start + WINDOW_SIZE

        window = holdout_df.iloc[start:end]

        feature_results = []
        drifted_features = []

        for feature in FEATURES:
            ref_values = pd.to_numeric(
                reference_df[feature],
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
                feature_results.append({
                    "feature": feature,
                    "status": "insufficient_data",
                })
                continue

            statistic, p_value = ks_2samp(
                ref_values,
                window_values,
            )

            drift_detected = (
                p_value < corrected_alpha
                and statistic >= MIN_KS_STATISTIC
            )

            if drift_detected:
                drifted_features.append(feature)

            feature_results.append({
                "feature": feature,
                "ks_statistic": float(statistic),
                "p_value": float(p_value),
                "drift_detected": bool(drift_detected),
            })

        results.append({
            "window": window_id + 1,
            "drifted_feature_count": len(drifted_features),
            "drifted_features": drifted_features,
            "features": feature_results,
        })

        print(
            f"Window {window_id + 1}: "
            f"{len(drifted_features)}/{len(FEATURES)} "
            "features drifted"
        )

    report = {
        "reference_capture": "all captures except PortScan",
        "test_capture": HOLDOUT,
        "reference_size": REFERENCE_SIZE,
        "window_size": WINDOW_SIZE,
        "number_of_windows": NUM_WINDOWS,
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

    flagged_windows = sum(
        result["drifted_feature_count"] > 0
        for result in results
    )

    print("\n=== DRIFT DETECTION SUMMARY ===")
    print("Windows evaluated:", NUM_WINDOWS)
    print("Windows with drift:", flagged_windows)
    print(
        "Drift detection rate:",
        round(flagged_windows / NUM_WINDOWS * 100, 2),
        "%",
    )
    print("Report saved:", OUTPUT_PATH)


if __name__ == "__main__":
    main()
