"""Validate reference capture separation against a split manifest."""
import argparse
import hashlib
import json
from pathlib import Path
import pandas as pd


def validate_provenance(manifest_path, reference_path, holdout):
    manifest_path, reference_path = Path(manifest_path), Path(reference_path)
    for path in (manifest_path, reference_path):
        if not path.is_file():
            raise ValueError(f"Missing input: {path}")
    groups = json.loads(manifest_path.read_text())["groups"]
    names = ("reference", "calibration", "evaluation_benign")
    if any(name not in groups for name in names):
        raise ValueError("Missing required split group")
    sets = {}
    for name in names:
        captures = groups[name]["captures"]
        if not isinstance(captures, dict) or not captures:
            raise ValueError(f"Invalid capture group: {name}")
        sets[name] = set(captures)
    for i, left in enumerate(names):
        for right in names[i + 1:]:
            if not sets[left].isdisjoint(sets[right]):
                raise ValueError(f"Capture group overlap: {left}, {right}")
    if holdout in sets["reference"] or holdout not in sets["evaluation_benign"]:
        raise ValueError("Holdout capture separation failed")
    frame = pd.read_csv(reference_path, usecols=["SourceFile", "Label"])
    if frame.empty or frame[["SourceFile", "Label"]].isna().any().any():
        raise ValueError("Empty or missing reference metadata")
    if not frame["Label"].astype(str).str.strip().eq("BENIGN").all():
        raise ValueError("Reference must contain only BENIGN labels")
    counts = {str(k): int(v) for k, v in frame["SourceFile"].astype(str).str.strip().value_counts().items()}
    if set(counts) != sets["reference"]:
        raise ValueError("Reference capture membership mismatch")
    if counts != groups["reference"]["captures"]:
        raise ValueError("Reference capture counts disagree with manifest")
    if len(frame) != groups["reference"]["total_rows"]:
        raise ValueError("Reference row count disagrees with manifest")
    return {"passed": True, "reference_rows": len(frame),
            "reference_capture_counts": counts,
            "reference_sha256": hashlib.sha256(reference_path.read_bytes()).hexdigest(),
            "holdout": holdout, "holdout_excluded": True,
            "capture_groups_disjoint": True,
            "limitation": "Capture-level check only; not raw-row or event-time proof"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("data/processed/drift/split_manifest.json"))
    parser.add_argument("--reference", type=Path, default=Path("data/processed/drift/reference.csv"))
    parser.add_argument("--holdout", default="Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv")
    args = parser.parse_args()
    print(json.dumps(validate_provenance(args.manifest, args.reference, args.holdout), indent=2))


if __name__ == "__main__":
    main()
