
from pathlib import Path
import json

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

BASE = Path("data/raw/cicids2017/MachineLearningCVE")
MONDAY = BASE / "Monday-WorkingHours.pcap_ISCX.csv"
FRIDAY = BASE / "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv"

OUTPUT = Path("models/drift_friday_groups.json")

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
    corrected_alpha = ALPHA / len(FEATURES)
    results = []

    for i in range(NUM_WINDOWS):
        window = test.iloc[
            i * WINDOW_SIZE:(i + 1) * WINDOW_SIZE
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
            "window": i + 1,
            "drifted_feature_count": len(drifted),
            "drifted_features": drifted,
        })

    flagged = sum(
        result["drifted_feature_count"] > 0
        for result in results
    )

    print(f"{name}: {flagged}/{NUM_WINDOWS} flagged")

    return {
        "flagged_windows": flagged,
        "total_windows": NUM_WINDOWS,
        "flag_rate": flagged / NUM_WINDOWS,
        "windows": results,
    }


def main():
    rng = np.random.default_rng(SEED)

    monday = load_data(MONDAY)
    friday = load_data(FRIDAY)

    monday = monday[monday["Label"] == "BENIGN"]
    friday_benign = friday[friday["Label"] == "BENIGN"]
    friday_attack = friday[friday["Label"] == "PortScan"]

    required = WINDOW_SIZE * NUM_WINDOWS

    if (
        len(monday) < REFERENCE_SIZE
        or len(friday_benign) < required
        or len(friday_attack) < required
    ):
        raise RuntimeError("Insufficient records")

    reference = monday.iloc[
        rng.choice(len(monday), REFERENCE_SIZE, replace=False)
    ]

    benign_test = friday_benign.iloc[
        rng.choice(len(friday_benign), required, replace=False)
    ]

    attack_test = friday_attack.iloc[
        rng.choice(len(friday_attack), required, replace=False)
    ]

    benign_results = evaluate(
        reference, benign_test, "Friday BENIGN"
    )

    attack_results = evaluate(
        reference, attack_test, "Friday PortScan"
    )

    report = {
        "reference": "Monday BENIGN",
        "reference_size": REFERENCE_SIZE,
        "window_size": WINDOW_SIZE,
        "number_of_windows": NUM_WINDOWS,
        "benign": benign_results,
        "attack": attack_results,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, indent=2))

    print("\n=== FRIDAY GROUP COMPARISON ===")
    print(
        "Friday benign flag rate:",
        round(benign_results["flag_rate"] * 100, 2),
        "%",
    )
    print(
        "Friday PortScan flag rate:",
        round(attack_results["flag_rate"] * 100, 2),
        "%",
    )
    print("Report saved:", OUTPUT)


if __name__ == "__main__":
    main()
