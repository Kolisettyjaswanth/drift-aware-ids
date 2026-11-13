
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path("data/raw/cicids2017/MachineLearningCVE")

FILE = BASE / "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"

FEATURES = [
    "Average Packet Size",
    "Destination Port",
    "Packet Length Mean",
]

WINDOW_SIZE = 10000


def main():
    df = pd.read_csv(
        FILE,
        usecols=lambda col: col.strip() in FEATURES + ["Label"],
        encoding="utf-8-sig",
        low_memory=False,
    )

    df.columns = df.columns.str.strip()
    df["Label"] = df["Label"].astype(str).str.strip()

    # Preserve original CSV row order.
    # This is a proxy for capture order, not verified timestamps.
    df = df[df["Label"] == "BENIGN"].copy()

    for feature in FEATURES:
        df[feature] = pd.to_numeric(
            df[feature], errors="coerce"
        ).replace([np.inf, -np.inf], np.nan)

    print("Total benign records:", len(df))
    print("Window size:", WINDOW_SIZE)

    for start in range(0, len(df), WINDOW_SIZE):
        window = df.iloc[start:start + WINDOW_SIZE]

        if len(window) < WINDOW_SIZE:
            break

        window_id = start // WINDOW_SIZE + 1

        print(f"\n=== Window {window_id} ===")

        for feature in FEATURES:
            values = window[feature].dropna()

            print(
                f"{feature}: "
                f"median={values.median():.2f}, "
                f"p90={values.quantile(0.90):.2f}"
            )


if __name__ == "__main__":
    main()
