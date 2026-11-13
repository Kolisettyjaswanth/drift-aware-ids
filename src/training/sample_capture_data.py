
from pathlib import Path

import numpy as np
import pandas as pd


DATA_PATH = Path(
    "data/processed/capture_aware_cicids2017.csv"
)

CHUNK_SIZE = 50000
MAX_TRAIN_ROWS = 300000
SEED = 42


def sample_training_data(holdout_capture):
    """
    Sample training records while excluding the
    specified holdout capture.

    Memory remains bounded approximately by
    MAX_TRAIN_ROWS plus one input chunk.
    """

    rng = np.random.default_rng(SEED)

    sample = None
    processed_rows = 0

    for chunk_number, chunk in enumerate(
        pd.read_csv(
            DATA_PATH,
            chunksize=CHUNK_SIZE,
            low_memory=False,
        ),
        start=1,
    ):
        chunk.columns = chunk.columns.str.strip()

        # Exclude the holdout capture completely.
        chunk = chunk[
            chunk["SourceFile"] != holdout_capture
        ].copy()

        if chunk.empty:
            continue

        # Assign random priorities to records.
        chunk["_sample_key"] = rng.random(len(chunk))

        # Keep only the smallest priorities.
        if len(chunk) > MAX_TRAIN_ROWS:
            chunk = chunk.nsmallest(
                MAX_TRAIN_ROWS,
                "_sample_key",
            )

        if sample is None:
            sample = chunk
        else:
            sample = pd.concat(
                [sample, chunk],
                ignore_index=True,
            )

            if len(sample) > MAX_TRAIN_ROWS:
                sample = sample.nsmallest(
                    MAX_TRAIN_ROWS,
                    "_sample_key",
                ).copy()

        processed_rows += len(chunk)

        print(
            f"Chunk {chunk_number}: "
            f"eligible records={processed_rows:,}, "
            f"sample size={len(sample):,}",
            flush=True,
        )

    if sample is None or sample.empty:
        raise ValueError(
            "No eligible training records found."
        )

    sample = sample.drop(
        columns=["_sample_key"]
    ).reset_index(drop=True)

    if holdout_capture in set(sample["SourceFile"]):
        raise AssertionError(
            "Holdout capture leaked into training data!"
        )

    return sample


def main():
    holdout = (
        "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv"
    )

    print("Sampling training records...")

    sample = sample_training_data(holdout)

    print("\n=== SAMPLING VERIFICATION ===")
    print("Training rows:", len(sample))
    print("Captures:", sample["SourceFile"].nunique())
    print(
        "Holdout present:",
        holdout in set(sample["SourceFile"]),
    )
    print(
        "Labels:",
        sample["Label"].value_counts().to_dict(),
    )


if __name__ == "__main__":
    main()
