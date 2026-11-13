
from pathlib import Path
import json

import numpy as np
import pandas as pd
from src.drift.ks import legacy_ks_2samp as ks_2samp, feature_alert

BASE = Path("data/raw/cicids2017/MachineLearningCVE")

MONDAY = BASE / "Monday-WorkingHours.pcap_ISCX.csv"
FRIDAY_DDOS = BASE / "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"

OUTPUT = Path("models/drift_ddos_evaluation.json")

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

MIN_DRIFTED_FEATURES = 2
MIN_KS_STATISTIC = 0.10
ALPHA = 0.05
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

    for feature in FEATURES:
        df[feature] = pd.to_numeric(
            df[feature], errors="coerce"
        ).replace([np.inf, -np.inf], np.nan)

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
            a = reference[feature].dropna()
            b = window[feature].dropna()

            if len(a) < 100 or len(b) < 100:
                continue

            statistic, p_value = ks_2samp(a, b)

            if (
                feature_alert(statistic, p_value, alpha=corrected_alpha, min_ks=MIN_KS_STATISTIC)
            ):
                drifted.append(feature)

        alert = len(drifted) >= MIN_DRIFTED_FEATURES

        results.append({
            "window": i + 1,
            "drifted_feature_count": len(drifted),
            "drifted_features": drifted,
            "alert": alert,
        })

    flagged = sum(item["alert"] for item in results)

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
    friday = load_data(FRIDAY_DDOS)

    monday = monday[monday["Label"] == "BENIGN"]
    friday_benign = friday[friday["Label"] == "BENIGN"]
    friday_attack = friday[friday["Label"] == "DDoS"]

    required = WINDOW_SIZE * NUM_WINDOWS

    for name, frame, needed in [
        ("Monday reference", monday, REFERENCE_SIZE),
        ("Friday benign", friday_benign, required),
        ("Friday DDoS", friday_attack, required),
    ]:
        if len(frame) < needed:
            raise RuntimeError(f"Insufficient records: {name}")

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
        reference, benign_test, "Friday DDoS capture BENIGN"
    )

    attack_results = evaluate(
        reference, attack_test, "Friday DDoS"
    )

    report = {
        "reference": "Monday BENIGN",
        "evaluation_capture": FRIDAY_DDOS.name,
        "reference_size": REFERENCE_SIZE,
        "window_size": WINDOW_SIZE,
        "number_of_windows": NUM_WINDOWS,
        "minimum_drifted_features": MIN_DRIFTED_FEATURES,
        "minimum_ks_statistic": MIN_KS_STATISTIC,
        "alpha": ALPHA,
        "benign": benign_results,
        "attack": attack_results,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, indent=2))

    print("\n=== DDoS CAPTURE EVALUATION ===")
    print(
        "Benign flag rate:",
        round(benign_results["flag_rate"] * 100, 2), "%"
    )
    print(
        "DDoS flag rate:",
        round(attack_results["flag_rate"] * 100, 2), "%"
    )
    print("Report saved:", OUTPUT)


if __name__ == "__main__":
    main()

