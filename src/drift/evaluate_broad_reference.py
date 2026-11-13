
import json
from pathlib import Path

import pandas as pd
from scipy.stats import ks_2samp


# --------------------------------------------------
# 1. File locations
# --------------------------------------------------

DATA_DIR = Path("data/processed/drift")

REFERENCE_FILE = DATA_DIR / "reference.csv"
CALIBRATION_FILE = DATA_DIR / "calibration.csv"

OUTPUT_FILE = Path(
    "models/drift_broad_reference_calibration.json"
)


# --------------------------------------------------
# 2. Features used for drift detection
# --------------------------------------------------

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


# --------------------------------------------------
# 3. Drift detection settings
# --------------------------------------------------

WINDOW_SIZE = 1000

P_THRESHOLD = 0.05 / len(FEATURES)

KS_THRESHOLD = 0.10

MIN_DRIFTED_FEATURES = 2


# --------------------------------------------------
# 4. Evaluate one capture
# --------------------------------------------------

def evaluate_capture(reference, capture_df):

    windows = []

    for start in range(
        0,
        len(capture_df),
        WINDOW_SIZE
    ):

        window = capture_df.iloc[
            start:start + WINDOW_SIZE
        ]

        # Ignore incomplete windows
        if len(window) < WINDOW_SIZE:
            continue

        drifted_features = []

        # Compare each feature distribution
        for feature in FEATURES:

            statistic, p_value = ks_2samp(
                reference[feature],
                window[feature],
                method="asymp"
            )

            # Feature is considered drifted only
            # when both conditions are satisfied
            if (
                p_value < P_THRESHOLD
                and statistic >= KS_THRESHOLD
            ):
                drifted_features.append(feature)

        # Decide whether this window raises an alert
        alert = (
            len(drifted_features)
            >= MIN_DRIFTED_FEATURES
        )

        windows.append({
            "window_index": len(windows) + 1,
            "drifted_feature_count": len(drifted_features),
            "drifted_features": drifted_features,
            "alert": alert,
        })

    return windows


# --------------------------------------------------
# 5. Main program
# --------------------------------------------------

def main():

    print("Loading reference dataset...")

    reference = pd.read_csv(REFERENCE_FILE)

    print("Loading calibration dataset...")

    calibration = pd.read_csv(CALIBRATION_FILE)

    # Check required columns
    required_columns = FEATURES + ["Label", "SourceFile"]

    for name, df in [
        ("reference", reference),
        ("calibration", calibration),
    ]:
        missing = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing:
            raise ValueError(
                f"{name} dataset missing columns: {missing}"
            )

        if not df["Label"].eq("BENIGN").all():
            raise ValueError(
                f"{name} dataset contains non-BENIGN records"
            )

        if df[FEATURES].isna().any().any():
            raise ValueError(
                f"{name} dataset contains missing feature values"
            )

    # Prevent reference/calibration capture overlap
    reference_captures = set(reference["SourceFile"])
    calibration_captures = set(calibration["SourceFile"])

    if reference_captures & calibration_captures:
        raise ValueError(
            "Reference and calibration captures overlap!"
        )

    report = {
        "window_size": WINDOW_SIZE,
        "p_threshold": P_THRESHOLD,
        "ks_threshold": KS_THRESHOLD,
        "min_drifted_features": MIN_DRIFTED_FEATURES,
        "reference_captures": sorted(reference_captures),
        "captures": {},
    }

    # Evaluate each calibration capture separately
    for capture, group in calibration.groupby(
        "SourceFile",
        sort=True
    ):

        windows = evaluate_capture(
            reference,
            group
        )

        total_windows = len(windows)

        if total_windows == 0:
            raise ValueError(
                f"No complete windows for {capture}"
            )

        alert_count = sum(
            window["alert"]
            for window in windows
        )

        alert_rate = alert_count / total_windows

        report["captures"][capture] = {
            "total_windows": total_windows,
            "alerted_windows": alert_count,
            "alert_rate": alert_rate,
            "windows": windows,
        }

        print(f"\n=== {capture} ===")

        print("Windows:", total_windows)

        print("Drift alerts:", alert_count)

        print(f"Alert rate: {alert_rate:.1%}")

        print(
            "Drifted feature counts:",
            [
                window["drifted_feature_count"]
                for window in windows
            ]
        )

    # Save the results
    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    OUTPUT_FILE.write_text(
        json.dumps(report, indent=2)
    )

    print("\nSaved report:", OUTPUT_FILE)

    print("Broad-reference calibration completed.")


if __name__ == "__main__":
    main()
