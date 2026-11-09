
from pathlib import Path
from collections import Counter
import json

import numpy as np
import pandas as pd

INPUT = Path("data/processed/cleaned_cicids2017.csv")
AUDIT = Path("data/processed/dataset_audit.json")
OUTPUT_DIR = Path("data/processed/ml")
CHUNK_SIZE = 50_000
SEED = 42

# Development sample, not the final evaluation dataset.
TARGET_ROWS = 300_000


def main():
    audit = json.loads(AUDIT.read_text())
    class_counts = audit["label_counts"]

    total = sum(class_counts.values())
    if total != audit["total_rows"]:
        raise ValueError("Audit label totals do not match row count")

    if TARGET_ROWS > total:
        raise ValueError("Target sample exceeds dataset size")

    rng = np.random.default_rng(SEED)

    # Allocate an approximate proportional quota per class.
    quotas = {
        label: min(
            count,
            max(2, int(TARGET_ROWS * count / total))
        )
        for label, count in class_counts.items()
    }

    print("Sampling quotas:")
    for label, quota in quotas.items():
        print(f"{label}: {quota:,}")

    remaining_rows = dict(class_counts)
    remaining_quota = dict(quotas)
    selected_chunks = []

    for chunk in pd.read_csv(
        INPUT,
        chunksize=CHUNK_SIZE,
        low_memory=False,
    ):
        labels = chunk["Label"].astype(str).str.strip()
        selected_positions = []

        for position, label in enumerate(labels):
            if label not in remaining_rows:
                raise ValueError(f"Unexpected label: {label}")

            rows_left = remaining_rows[label]
            quota_left = remaining_quota[label]

            # Sequential sampling without replacement.
            if quota_left > 0 and rng.random() < quota_left / rows_left:
                selected_positions.append(position)
                remaining_quota[label] -= 1

            remaining_rows[label] -= 1

        if selected_positions:
            selected_chunks.append(
                chunk.iloc[selected_positions].copy()
            )

    if any(remaining_quota.values()):
        raise RuntimeError("Sampling quotas were not fulfilled")

    sample = pd.concat(selected_chunks, ignore_index=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sample_path = OUTPUT_DIR / "development_sample.csv"

    sample.to_csv(sample_path, index=False)

    metadata = {
        "seed": SEED,
        "requested_target_rows": TARGET_ROWS,
        "actual_sample_rows": len(sample),
        "sampling_method": "per-class sequential sampling",
        "class_counts": dict(Counter(sample["Label"])),
        "purpose": "development baseline only",
    }

    (OUTPUT_DIR / "sample_metadata.json").write_text(
        json.dumps(metadata, indent=2)
    )

    print("\n=== SAMPLE CREATED ===")
    print("Rows:", len(sample))
    print("Classes:", sample["Label"].nunique())
    print("Output:", sample_path)


if __name__ == "__main__":
    main()
