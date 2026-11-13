
from pathlib import Path
import json

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

BASE = Path("data/raw/cicids2017/MachineLearningCVE")
MONDAY = BASE / "Monday-WorkingHours.pcap_ISCX.csv"
PORTSCAN = BASE / "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv"

OUTPUT = Path("models/drift_matched_reference.json")

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

ALPHA = 0.05
MIN_KS_STATISTIC = 0.10
SEED = 42


def load_data(path):
    df = pd.read_csv(
        path,
        usecols=lambda col: col.strip() in FEATURES + ["Label"],
        low_memory=False,
        encoding="utf-8-sig",
    )
    df.columns = df.columns.str.strip()
    df["Label"] = df["Label"].astype(str).str.strip()
    return df


def evaluate(reference, test, name):
    results = []
    corrected_alpha = ALPHA / len(FEATURES)

    for window_id in range(NUM_WINDOWS):
        window = test.iloc[
            window_id * WINDOW_SIZE:
            (window_id + 1) * WINDOW_SIZE
        ]

        drifted = []

        for feature in FEATURES:
            a = pd.to_numeric(
                reference[feature], errors="coerce"
            ).replace([np.inf, -np.inf], np.nan).dropna()

            b = pd.to_numeric(
                window[feature], errors="coerce"
            ).replace([np.inf, -np.inf], np.nan).dropna()

            if len(a) < 100 or len(b) < 100:
                continue

            statistic, p_value = ks_2samp(a, b)

            if (
                p_value < corrected_alpha
                and statistic >= MIN_KS_STATISTIC
            ):
                drifted.append(feature)

        results.append({
            "window": window_id + 1,
            "drifted_features": drifted,
            "drifted_feature_count": len(drifted),
        })

    flagged = sum(
        item["drifted_feature_count"] > 0
        for item in results
    )

    print(f"{name}: {flagged}/{NUM_WINDOWS} windows flagged")

    return {
        "flagged_windows": flagged,
        "total_windows": NUM_WINDOWS,
        "flag_rate": flagged / NUM_WINDOWS,
        "windows": results,
    }


def main():
    rng = np.random.default_rng(SEED)

    monday = load_data(MONDAY)
    monday = monday[monday["Label"] == "BENIGN"]

    friday = load_data(PORTSCAN)

    required = REFERENCE_SIZE + WINDOW_SIZE * NUM_WINDOWS

    if len(monday) < required:
        raise RuntimeError("Insufficient Monday records")

    if len(friday) < WINDOW_SIZE * NUM_WINDOWS:
        raise RuntimeError("Insufficient Friday records")

    monday_indices = rng.choice(
        len(monday), size=required, replace=False
    )

    friday_indices = rng.choice(
        len(friday),
        size=WINDOW_SIZE * NUM_WINDOWS,
        replace=False,
    )

    reference = monday.iloc[
        monday_indices[:REFERENCE_SIZE]
    ]

    benign_test = monday.iloc[
        monday_indices[REFERENCE_SIZE:]
    ]

    portscan_test = friday.iloc[friday_indices]

    print("Reference records:", len(reference))
    print("Benign control records:", len(benign_test))
    print("Friday test records:", len(portscan_test))

    benign_results = evaluate(
        reference, benign_test, "Monday benign"
    )

    friday_results = evaluate(
        reference, portscan_test, "Friday PortScan capture"
    )

    report = {
        "reference": "Monday BENIGN",
        "reference_size": REFERENCE_SIZE,
        "window_size": WINDOW_SIZE,
        "number_of_windows": NUM_WINDOWS,
        "features": FEATURES,
        "alpha": ALPHA,
        "minimum_ks_statistic": MIN_KS_STATISTIC,
        "benign_control": benign_results,
        "friday_capture": friday_results,
        "friday_attack_fraction": float(
            (portscan_test["Label"] != "BENIGN").mean()
        ),
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, indent=2))

    print("\n=== MATCHED REFERENCE SUMMARY ===")
    print(
        "Monday benign flag rate:",
        round(benign_results["flag_rate"] * 100, 2),
        "%",
    )
    print(
        "Friday capture flag rate:",
        round(friday_results["flag_rate"] * 100, 2),
        "%",
    )
    print(
        "Friday sampled attack fraction:",
        round(report["friday_attack_fraction"] * 100, 2),
        "%",
    )
    print("Report saved:", OUTPUT)


if __name__ == "__main__":
    main()
