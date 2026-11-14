"""Offline tests for optional Spark batch inference persistence."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from src.streaming.spark_consumer import BatchSink
from src.streaming.validation import read_records
from tests.test_spark_streaming import Frame, row


def prediction_for(record):
    event = record["event"]
    return {
        "event_id": event["event_id"],
        "replay_id": event["replay_id"],
        "source_row": event["source_row"],
        "kafka": record["kafka"],
        "prediction": 0,
        "prediction_label": "BENIGN",
        "probability_benign": 1.0,
        "probability_attack": 0.0,
    }


def engine():
    model = Mock()
    model.predict_records.side_effect = lambda records: [
        prediction_for(record) for record in records
    ]
    return model


def predictions(root):
    directory = Path(root) / "predictions"
    result = []
    for path in sorted(directory.glob("batch-*.jsonl")):
        for line in path.read_text().splitlines():
            result.append(json.loads(line))
    return result


class InferenceSinkTests(unittest.TestCase):

    def test_predictions_persisted(self):
        with tempfile.TemporaryDirectory() as d:
            model = engine()
            sink = BatchSink(d, 10, inference_engine=model)

            sink(Frame([row(0), row(1)]), 0)

            self.assertEqual(len(list(read_records(d))), 2)
            self.assertEqual(len(predictions(d)), 2)
            model.predict_records.assert_called_once()

    def test_retry_replaces_predictions(self):
        with tempfile.TemporaryDirectory() as d:
            sink = BatchSink(d, 10, inference_engine=engine())
            frame = Frame([row(0), row(1)])

            sink(frame, 0)
            sink(frame, 0)

            self.assertEqual(len(list(read_records(d))), 2)
            self.assertEqual(len(predictions(d)), 2)
            self.assertEqual(len(sink.sizes), 1)

    def test_invalid_record_excluded_from_predictions(self):
        with tempfile.TemporaryDirectory() as d:
            invalid = row(1)
            invalid["value"] = b"bad"

            sink = BatchSink(d, 10, inference_engine=engine())
            sink(Frame([row(0), invalid]), 0)

            records = list(read_records(d))
            self.assertEqual(len(records), 2)
            self.assertEqual(
                sum(record["valid"] is False for record in records),
                1,
            )
            self.assertEqual(len(predictions(d)), 1)

    def test_model_failure_does_not_commit_transport(self):
        with tempfile.TemporaryDirectory() as d:
            model = engine()
            model.predict_records.side_effect = RuntimeError(
                "Simulated model failure"
            )
            sink = BatchSink(d, 10, inference_engine=model)

            with self.assertRaisesRegex(
                RuntimeError, "Simulated model failure"
            ):
                sink(Frame([row(0)]), 0)

            self.assertEqual(list(read_records(d)), [])
            self.assertEqual(predictions(d), [])
            self.assertEqual(sink.sizes, {})

    def test_safety_cap_failure_does_not_commit(self):
        with tempfile.TemporaryDirectory() as d:
            sink = BatchSink(d, 1, inference_engine=engine())

            with self.assertRaisesRegex(
                ValueError, "safety cap"
            ):
                sink(Frame([row(0), row(1)]), 0)

            self.assertEqual(list(read_records(d)), [])
            self.assertEqual(predictions(d), [])

    def test_all_invalid_batch_creates_empty_prediction_file(self):
        with tempfile.TemporaryDirectory() as d:
            invalid = row(0)
            invalid["value"] = b"bad"

            model = engine()
            sink = BatchSink(d, 10, inference_engine=model)
            sink(Frame([invalid]), 0)

            self.assertEqual(len(list(read_records(d))), 1)
            self.assertEqual(predictions(d), [])
            self.assertEqual(
                len(list(
                    (Path(d) / "predictions").glob("batch-*.jsonl")
                )),
                1,
            )
            model.predict_records.assert_not_called()


if __name__ == "__main__":
    unittest.main()
