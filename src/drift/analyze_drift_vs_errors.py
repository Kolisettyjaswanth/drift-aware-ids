
from pathlib import Path
import json

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp


# ==========================================
# CONFIGURATION
# ==========================================

REFERENCE_PATH = Path(
    "data/processed/drift/reference.csv"
)

PREDICTIONS_PATH = Path(
    "data/processed/drift/"
    "heldout_portscan_predictions.csv"
)

OUTPUT_PATH = Path(
    "models/drift_vs_errors_portscan.json"
)

WINDOW_SIZE = 1000

KS_THRESHOLD = 0.10

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


# ==========================================
# MAIN ANALYSIS
# ==========================================

def main():

    print("Loading reference dataset...")

    reference = pd.read_csv(
        REFERENCE_PATH
    )

    print("Loading held-out predictions...")

    predictions = pd.read_csv(
        PREDICTIONS_PATH
    )

    # Verify required columns.

    required = set(FEATURES) | {
        "Actual",
        "Predicted",
        "Correct",
        "Label",
    }

    for name, df in [
        ("reference", reference),
        ("predictions", predictions),
    ]:
        missing = set(FEATURES) - set(df.columns)

        if missing:
            raise ValueError(
                f"{name} missing features: {missing}"
            )

    if not required.issubset(predictions.columns):
        raise ValueError(
            "Prediction file is missing required columns."
        )

    # Check for invalid numeric values.

    for name, df in [
        ("reference", reference),
        ("predictions", predictions),
    ]:
        values = df[FEATURES].to_numpy(
            dtype=float
        )

        if not np.isfinite(values).all():
            raise ValueError(
                f"{name} contains NaN or infinite "
                "drift feature values."
            )

    total_windows = (
        len(predictions) // WINDOW_SIZE
    )

    print(
        "\nComplete windows:",
        total_windows
    )

    print(
        "Excluded trailing records:",
        len(predictions) % WINDOW_SIZE
    )

    # ======================================
    # COMPARE EACH WINDOW
    # ======================================

    window_results = []

    for window_index in range(total_windows):

        start = window_index * WINDOW_SIZE
        end = start + WINDOW_SIZE

        window = predictions.iloc[
            start:end
        ]

        drifted_features = []
        ks_statistics = []

        for feature in FEATURES:

            statistic, p_value = ks_2samp(
                reference[feature],
                window[feature],
                method="asymp",
            )

            ks_statistics.append(
                float(statistic)
            )

            if (
                p_value < P_THRESHOLD
                and statistic >= KS_THRESHOLD
            ):
                drifted_features.append(
                    feature
                )

        actual = window["Actual"]
        predicted = window["Predicted"]

        attack_mask = actual.eq(1)
        benign_mask = actual.eq(0)

        attack_count = int(
            attack_mask.sum()
        )

        benign_count = int(
            benign_mask.sum()
        )

        error_rate = float(
            (actual != predicted).mean()
        )

        if attack_count > 0:
            attack_recall = float(
                predicted[attack_mask].eq(1).mean()
            )
        else:
            attack_recall = None

        if benign_count > 0:
            benign_fpr = float(
                predicted[benign_mask].eq(1).mean()
            )
        else:
            benign_fpr = None

        result = {
            "window_index": window_index,
            "start_row": start,
            "end_row_exclusive": end,
            "records": len(window),
            "attack_count": attack_count,
            "benign_count": benign_count,
            "attack_fraction": (
                attack_count / len(window)
            ),
            "drifted_feature_count": len(
                drifted_features
            ),
            "drifted_features": drifted_features,
            "mean_ks": float(
                np.mean(ks_statistics)
            ),
            "drift_alert": (
                len(drifted_features) >= 2
            ),
            "error_rate": error_rate,
            "attack_recall": attack_recall,
            "benign_false_positive_rate": benign_fpr,
        }

        window_results.append(result)

        if (window_index + 1) % 50 == 0:
            print(
                "Processed windows:",
                window_index + 1
            )

    # ======================================
    # SUMMARY
    # ======================================

    results_df = pd.DataFrame(
        window_results
    )

    alert_mask = results_df[
        "drift_alert"
    ].astype(bool)

    alerted = results_df.loc[
        alert_mask
    ]

    not_alerted = results_df.loc[
        ~alert_mask
    ]

    # Correlation is descriptive only.
    # A constant variable has no correlation.

    if (
        results_df["mean_ks"].nunique() > 1
        and results_df["error_rate"].nunique() > 1
    ):
        correlation = float(
            results_df["mean_ks"].corr(
                results_df["error_rate"]
            )
        )
    else:
        correlation = None

    def average_or_none(frame, column):
        if frame.empty:
            return None

        return float(
            frame[column].mean()
        )

    summary = {
        "total_records": int(
            len(predictions)
        ),
        "window_size": WINDOW_SIZE,
        "complete_windows": total_windows,
        "excluded_trailing_records": int(
            len(predictions) % WINDOW_SIZE
        ),
        "alerted_windows": int(
            alert_mask.sum()
        ),
        "non_alerted_windows": int(
            (~alert_mask).sum()
        ),
        "alert_rate": float(
            alert_mask.mean()
        ),
        "overall_window_error_rate": float(
            results_df["error_rate"].mean()
        ),
        "alerted_window_mean_error": (
            average_or_none(
                alerted,
                "error_rate"
            )
        ),
        "non_alerted_window_mean_error": (
            average_or_none(
                not_alerted,
                "error_rate"
            )
        ),
        "mean_ks_error_correlation": correlation,
    }

    report = {
        "experiment": (
            "Held-out PortScan drift "
            "versus classifier errors"
        ),
        "reference_file": str(
            REFERENCE_PATH
        ),
        "predictions_file": str(
            PREDICTIONS_PATH
        ),
        "thresholds": {
            "ks_statistic": KS_THRESHOLD,
            "bonferroni_p": P_THRESHOLD,
            "minimum_drifted_features": 2,
        },
        "summary": summary,
        "windows": window_results,
        "limitations": [
            "Record-count windows are not "
            "verified chronological windows.",
            "PortScan is unseen during model training.",
            "Correlation does not establish causation.",
            "Attack prevalence can confound "
            "the drift-error relationship.",
        ],
    }

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    OUTPUT_PATH.write_text(
        json.dumps(
            report,
            indent=2,
            allow_nan=False,
        )
    )

    # ======================================
    # DISPLAY RESULTS
    # ======================================

    print(
        "\n=== DRIFT VS CLASSIFIER ERRORS ==="
    )

    print(
        "Total records:",
        summary["total_records"]
    )

    print(
        "Complete windows:",
        summary["complete_windows"]
    )

    print(
        "Alerted windows:",
        summary["alerted_windows"]
    )

    print(
        "Non-alerted windows:",
        summary["non_alerted_windows"]
    )

    print(
        "Drift alert rate:",
        round(
            summary["alert_rate"] * 100,
            2
        ),
        "%"
    )

    print(
        "Mean classification error:",
        round(
            summary["overall_window_error_rate"] * 100,
            2
        ),
        "%"
    )

    print(
        "Mean error in alerted windows:",
        summary["alerted_window_mean_error"]
    )

    print(
        "Mean error in non-alerted windows:",
        summary["non_alerted_window_mean_error"]
    )

    print(
        "Mean KS vs error correlation:",
        summary["mean_ks_error_correlation"]
    )

    print(
        "\nReport saved to:",
        OUTPUT_PATH
    )


if __name__ == "__main__":
    main()
