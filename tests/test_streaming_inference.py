"""Tests for Random Forest streaming inference."""

import copy
import unittest
from unittest.mock import patch

import numpy as np

from src.streaming.inference import StreamingInference


class StreamingInferenceTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = StreamingInference()

        features = {
            name: 0.0
            for name in cls.engine.features
        }

        cls.records = [
            {
                "valid": True,
                "kafka": {
                    "topic": "test-inference",
                    "partition": 0,
                    "offset": index,
                },
                "event": {
                    "schema_version": 1,
                    "event_id": f"test-replay:{index}",
                    "replay_id": "test-replay",
                    "source_row": index,
                    "features": features.copy(),
                },
            }
            for index in range(3)
        ]

    def test_empty_batch(self):
        self.assertEqual(
            self.engine.predict_records([]),
            []
        )

    def test_predictions_and_probabilities(self):
        results = self.engine.predict_records(
            self.records
        )

        self.assertEqual(
            len(results),
            len(self.records)
        )

        for result in results:
            self.assertIn(
                result["prediction"],
                (0, 1)
            )

            self.assertEqual(
                result["prediction_label"],
                "ATTACK"
                if result["prediction"] == 1
                else "BENIGN"
            )

            probabilities = [
                result["probability_benign"],
                result["probability_attack"],
            ]

            self.assertTrue(
                np.isfinite(probabilities).all()
            )

            self.assertTrue(
                np.isclose(sum(probabilities), 1.0)
            )

    def test_event_identity_preserved(self):
        results = self.engine.predict_records(
            self.records[:3]
        )

        for original, result in zip(
            self.records[:3],
            results
        ):
            self.assertEqual(
                result["event_id"],
                original["event"]["event_id"]
            )

            self.assertEqual(
                result["replay_id"],
                original["event"]["replay_id"]
            )

            self.assertEqual(
                result["source_row"],
                original["event"]["source_row"]
            )

            self.assertEqual(
                result["kafka"],
                original["kafka"]
            )

    def test_invalid_record_rejected(self):
        record = copy.deepcopy(self.records[0])
        record["valid"] = False

        with self.assertRaises(ValueError):
            self.engine.predict_records([record])

    def test_missing_feature_rejected(self):
        record = copy.deepcopy(self.records[0])

        missing_name = self.engine.features[0]
        del record["event"]["features"][missing_name]

        with self.assertRaisesRegex(
            ValueError,
            "Missing required model features"
        ):
            self.engine.predict_records([record])

    def test_null_feature_handled(self):
        record = copy.deepcopy(self.records[0])

        record["event"]["features"][
            self.engine.features[0]
        ] = None

        results = self.engine.predict_records(
            [record]
        )

        self.assertEqual(len(results), 1)
        self.assertIn(
            results[0]["prediction"],
            (0, 1)
        )


    def test_wrong_prediction_shape_rejected(self):
        with patch.object(
            self.engine.model,
            "predict",
            return_value=np.array([0, 1])
        ):
            with self.assertRaisesRegex(
                ValueError, "Unexpected prediction output shape"
            ):
                self.engine.predict_records(self.records[:1])

    def test_wrong_probability_shape_rejected(self):
        with patch.object(
            self.engine.model,
            "predict_proba",
            return_value=np.array([[1.0]])
        ):
            with self.assertRaisesRegex(
                ValueError, "Unexpected probability output shape"
            ):
                self.engine.predict_records(self.records[:1])

    def test_nonfinite_probability_rejected(self):
        with patch.object(
            self.engine.model,
            "predict_proba",
            return_value=np.array([[np.nan, 0.5]])
        ):
            with self.assertRaisesRegex(
                ValueError, "Non-finite model probabilities"
            ):
                self.engine.predict_records(self.records[:1])

    def test_probability_out_of_range_rejected(self):
        with patch.object(
            self.engine.model,
            "predict_proba",
            return_value=np.array([[1.2, -0.2]])
        ):
            with self.assertRaisesRegex(
                ValueError, "Model probabilities outside"
            ):
                self.engine.predict_records(self.records[:1])

    def test_probability_sum_rejected(self):
        with patch.object(
            self.engine.model,
            "predict_proba",
            return_value=np.array([[0.4, 0.4]])
        ):
            with self.assertRaisesRegex(
                ValueError, "Model probabilities do not sum"
            ):
                self.engine.predict_records(self.records[:1])

    def test_unknown_prediction_class_rejected(self):
        with patch.object(
            self.engine.model,
            "predict",
            return_value=np.array([2])
        ):
            with self.assertRaisesRegex(
                ValueError, "Unexpected prediction class"
            ):
                self.engine.predict_records(self.records[:1])


if __name__ == "__main__":
    unittest.main()
