"""Regression tests for committed inference verification."""

import json
import tempfile
import unittest
from pathlib import Path

from src.streaming.spark_consumer import BatchSink
from src.streaming.verify_inference import verify_inference
from tests.test_inference_sink import engine
from tests.test_spark_streaming import Frame, row


class VerifyInferenceTests(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

        sink = BatchSink(
            self.root,
            max_records=10,
            inference_engine=engine(),
        )
        sink(Frame([row(0), row(1)]), 0)

        self.prediction_file = next(
            (self.root / "predictions").glob("batch-*.jsonl")
        )

    def update_prediction(self, key, value):
        records = [
            json.loads(line)
            for line in self.prediction_file.read_text().splitlines()
        ]
        records[0][key] = value
        self.prediction_file.write_text(
            "".join(json.dumps(record) + "\n" for record in records)
        )

    def test_valid_output(self):
        result = verify_inference(self.root)
        self.assertTrue(result["verified"])
        self.assertEqual(result["predictions"], 2)

    def test_missing_prediction_file(self):
        self.prediction_file.unlink()

        with self.assertRaisesRegex(ValueError, "batch file mismatch"):
            verify_inference(self.root)

    def test_reordered_predictions(self):
        records = self.prediction_file.read_text().splitlines()
        self.prediction_file.write_text(
            "\n".join(reversed(records)) + "\n"
        )

        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            verify_inference(self.root)

    def test_invalid_probability_sum(self):
        self.update_prediction("probability_attack", 0.5)

        with self.assertRaisesRegex(ValueError, "Invalid probabilities"):
            verify_inference(self.root)

    def test_nonfinite_probability(self):
        self.update_prediction("probability_attack", float("nan"))

        with self.assertRaisesRegex(ValueError, "Invalid probabilities"):
            verify_inference(self.root)

    def test_missing_probability(self):
        records = [
            json.loads(line)
            for line in self.prediction_file.read_text().splitlines()
        ]
        del records[0]["probability_attack"]
        self.prediction_file.write_text(
            "".join(json.dumps(record) + "\n" for record in records)
        )

        with self.assertRaisesRegex(ValueError, "Malformed probabilities"):
            verify_inference(self.root)

    def test_empty_directories_rejected(self):
        for directory in ("batches", "predictions"):
            for path in (self.root / directory).glob("batch-*.jsonl"):
                path.unlink()

        with self.assertRaisesRegex(ValueError, "No committed"):
            verify_inference(self.root)

    def test_prediction_class_mismatch(self):
        self.update_prediction("prediction_label", "ATTACK")

        with self.assertRaisesRegex(ValueError, "label mismatch"):
            verify_inference(self.root)


if __name__ == "__main__":
    unittest.main()
