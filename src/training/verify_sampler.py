
import hashlib

import pandas as pd

from src.training.sample_capture_data import (
    sample_training_data,
)


HOLDOUT = (
    "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv"
)


def fingerprint(df):
    """
    Create a deterministic fingerprint of a DataFrame.
    """

    # Hash the content of every row.
    row_hashes = pd.util.hash_pandas_object(
        df,
        index=False,
    )

    # Sort to avoid differences caused only by row order.
    sorted_hashes = row_hashes.sort_values().to_numpy()

    return hashlib.sha256(
        sorted_hashes.tobytes()
    ).hexdigest()


def main():
    print("Running sampler for the first time...")
    first = sample_training_data(HOLDOUT)

    print("\nRunning sampler for the second time...")
    second = sample_training_data(HOLDOUT)

    first_hash = fingerprint(first)
    second_hash = fingerprint(second)

    print("\n=== REPRODUCIBILITY CHECK ===")
    print("First sample rows:", len(first))
    print("Second sample rows:", len(second))
    print("First fingerprint:", first_hash)
    print("Second fingerprint:", second_hash)

    assert len(first) == len(second) == 300000
    assert first_hash == second_hash

    print("\nReproducibility check PASSED.")


if __name__ == "__main__":
    main()
