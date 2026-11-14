"""Inference adapter for already validated Spark batch records."""

from src.streaming.inference import StreamingInference


def predict_validated_batch(records, engine=None):
    """Predict valid records, retaining the original input order."""
    records = list(records)

    if not records:
        return []

    valid_records = [
        record for record in records
        if record.get("valid") is True
    ]

    if not valid_records:
        return []

    if engine is None:
        engine = StreamingInference()

    predictions = engine.predict_records(valid_records)

    if len(predictions) != len(valid_records):
        raise ValueError("Inference output count mismatch")

    for record, prediction in zip(valid_records, predictions):
        event = record["event"]

        if (
            prediction["event_id"] != event["event_id"]
            or prediction["replay_id"] != event["replay_id"]
            or prediction["source_row"] != event["source_row"]
            or prediction["kafka"] != record["kafka"]
        ):
            raise ValueError("Inference event identity mismatch")

    return predictions
