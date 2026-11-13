
from pathlib import Path

import pandas as pd
from src.drift.ks import legacy_ks_2samp as ks_2samp, feature_alert


DATA_DIR = Path("data/processed/drift")

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

P_THRESHOLD = 0.05 / len(FEATURES)
KS_THRESHOLD = 0.10


def main():
    reference = pd.read_csv(
        DATA_DIR / "reference.csv"
    )

    calibration = pd.read_csv(
        DATA_DIR / "calibration.csv"
    )

    for target_capture, target in calibration.groupby(
        "SourceFile",
        sort=True
    ):
        print(f"\n=== TARGET: {target_capture} ===")

        comparisons = []

        for ref_capture, ref_group in reference.groupby(
            "SourceFile",
            sort=True
        ):
            drifted = []
            statistics = []

            for feature in FEATURES:
                statistic, p_value = ks_2samp(
                    ref_group[feature],
                    target[feature],
                    method="asymp",
                )

                statistics.append(statistic)

                if (
                    feature_alert(statistic, p_value, alpha=P_THRESHOLD, min_ks=KS_THRESHOLD)
                ):
                    drifted.append(feature)

            comparisons.append({
                "capture": ref_capture,
                "drifted_count": len(drifted),
                "mean_ks": sum(statistics) / len(statistics),
            })

        comparisons.sort(
            key=lambda result: (
                result["drifted_count"],
                result["mean_ks"],
            )
        )

        for result in comparisons:
            print(
                f"{result['capture']}: "
                f"drifted={result['drifted_count']}/10, "
                f"mean_KS={result['mean_ks']:.4f}"
            )

        print(
            "\nClosest sampled reference capture:",
            comparisons[0]["capture"]
        )


if __name__ == "__main__":
    main()

