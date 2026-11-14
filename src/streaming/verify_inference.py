"""Verify committed transport batches against inference predictions."""

import json
import math
from pathlib import Path


def read_jsonl(path):
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            yield json.loads(line)


def verify_inference(run_dir):
    root = Path(run_dir)
    batch_dir = root / "batches"
    prediction_dir = root / "predictions"

    if not batch_dir.is_dir() or not prediction_dir.is_dir():
        raise ValueError("Missing transport or prediction directory")

    batch_files = {
        path.name: path
        for path in batch_dir.glob("batch-*.jsonl")
    }

    prediction_files = {
        path.name: path
        for path in prediction_dir.glob("batch-*.jsonl")
    }

    if batch_files.keys() != prediction_files.keys():
        raise ValueError("Transport/prediction batch file mismatch")

    if not batch_files:
        raise ValueError("No committed inference batches found")

    total_valid = 0
    total_predictions = 0

    for name in sorted(batch_files):
        records = list(read_jsonl(batch_files[name]))
        predictions = list(read_jsonl(prediction_files[name]))

        valid_records = [
            record for record in records
            if record.get("valid") is True
        ]

        if len(predictions) != len(valid_records):
            raise ValueError(
                f"Prediction count mismatch in {name}"
            )

        for record, prediction in zip(valid_records, predictions):
            event = record["event"]

            if (
                prediction["event_id"] != event["event_id"]
                or prediction["replay_id"] != event["replay_id"]
                or prediction["source_row"] != event["source_row"]
                or prediction["kafka"] != record["kafka"]
            ):
                raise ValueError(
                    f"Prediction identity mismatch in {name}"
                )

            if prediction["prediction"] not in (0, 1):
                raise ValueError(
                    f"Invalid prediction class in {name}"
                )

            if prediction["prediction_label"] != (
                "BENIGN" if prediction["prediction"] == 0
                else "ATTACK"
            ):
                raise ValueError(
                    f"Prediction label mismatch in {name}"
                )

            try:
                benign = prediction["probability_benign"]
                attack = prediction["probability_attack"]

                if (
                    type(benign) not in (int, float)
                    or type(attack) not in (int, float)
                    or not math.isfinite(benign)
                    or not math.isfinite(attack)
                    or not 0 <= benign <= 1
                    or not 0 <= attack <= 1
                    or not math.isclose(
                        benign + attack,
                        1.0,
                        rel_tol=1e-7,
                        abs_tol=1e-9,
                    )
                ):
                    raise ValueError("Invalid probabilities")

            except (KeyError, TypeError) as exc:
                raise ValueError(
                    f"Malformed probabilities in {name}"
                ) from exc

        total_valid += len(valid_records)
        total_predictions += len(predictions)

    return {
        "verified": True,
        "batches": len(batch_files),
        "valid_records": total_valid,
        "predictions": total_predictions,
    }
