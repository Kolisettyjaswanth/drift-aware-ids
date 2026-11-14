"""Tests for validated Spark batch inference adapter."""

import copy
import unittest
from unittest.mock import Mock

from src.streaming.inference_batch import predict_validated_batch


def make_record(index, valid=True):
    return {
        "valid": valid,
        "kafka": {
            "topic": "test-inference",
            "partition": 0,
            "offset": index,
        },
        "event": {
            "event_id": f"replay:{index}",
            "replay_id": "replay",
            "source_row": index,
            "features": {},
        },
    }


def make_prediction(record):
    return {
        "event_id": record["event"]["event_id"],
        "replay_id": record["event"]["replay_id"],
        "source_row": record["event"]["source_row"],
        "kafka": copy.deepcopy(record["kafka"]),
        "prediction": 0,
        "prediction_label": "BENIGN",
        "probability_benign": 1.0,
        "probability_attack": 0.0,
    }


class BatchInferenceTests(unittest.TestCase):

    def setUp(self):
        self.records = [make_record(i) for i in range(3)]
        self.engine = Mock()
        self.engine.predict_records.side_effect = (
            lambda records: [
                make_prediction(record)
                for record in records
            ]
        )

    def test_empty_batch(self):
        self.assertEqual(
            predict_validated_batch([], self.engine), []
        )
        self.engine.predict_records.assert_not_called()

    def test_valid_batch(self):
        results = predict_validated_batch(
            self.records, self.engine
        )
        self.assertEqual(len(results), 3)
        self.assertEqual(
            [r["event_id"] for r in results],
            [r["event"]["event_id"] for r in self.records],
        )

    def test_invalid_records_excluded(self):
        records = copy.deepcopy(self.records)
        records[0]["valid"] = False

        results = predict_validated_batch(
            records, self.engine
        )

        self.assertEqual(len(results), 2)
        self.assertEqual(
            [r["event_id"] for r in results],
            ["replay:1", "replay:2"],
        )

    def test_all_invalid(self):
        records = copy.deepcopy(self.records)
        for record in records:
            record["valid"] = False

        self.assertEqual(
            predict_validated_batch(records, self.engine),
            [],
        )
        self.engine.predict_records.assert_not_called()

    def test_reordered_predictions_rejected(self):
        self.engine.predict_records.side_effect = (
            lambda records: [
                make_prediction(record)
                for record in reversed(records)
            ]
        )

        with self.assertRaisesRegex(
            ValueError, "identity mismatch"
        ):
            predict_validated_batch(
                self.records, self.engine
            )

    def test_incomplete_predictions_rejected(self):
        self.engine.predict_records.side_effect = (
            lambda records: [
                make_prediction(records[0])
            ]
        )

        with self.assertRaisesRegex(
            ValueError, "count mismatch"
        ):
            predict_validated_batch(
                self.records, self.engine
            )

    def test_kafka_identity_mismatch_rejected(self):
        def wrong_offset(records):
            predictions = [
                make_prediction(record)
                for record in records
            ]
            predictions[0]["kafka"]["offset"] = 999
            return predictions

        self.engine.predict_records.side_effect = wrong_offset

        with self.assertRaisesRegex(
            ValueError, "identity mismatch"
        ):
            predict_validated_batch(
                self.records, self.engine
            )

    def test_model_failure_propagates(self):
        self.engine.predict_records.side_effect = (
            RuntimeError("Model inference failed")
        )

        with self.assertRaisesRegex(
            RuntimeError, "Model inference failed"
        ):
            predict_validated_batch(
                self.records, self.engine
            )


if __name__ == "__main__":
    unittest.main()
