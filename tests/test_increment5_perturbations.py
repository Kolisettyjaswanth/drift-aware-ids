import unittest

import numpy as np
import pandas as pd

from src.robustness.evaluate_perturbations import evaluate_perturbation


class ThresholdModel:
    def predict(self, frame):
        return (frame["packet_size"].to_numpy() >= 10).astype(int)


class PerturbationTests(unittest.TestCase):
    def setUp(self):
        self.features = pd.DataFrame({"packet_size": [9.0, 10.0, 2.0, 12.0], "other": [1, 2, 3, 4]})
        self.labels = np.array([1, 1, 0, 0])

    def run_eval(self, strength=0.1):
        return evaluate_perturbation(ThresholdModel(), self.features, self.labels, "packet_size", strength)

    def test_deterministic(self):
        self.assertEqual(self.run_eval(), self.run_eval())

    def test_does_not_mutate_input(self):
        original = self.features.copy(deep=True)
        self.run_eval()
        pd.testing.assert_frame_equal(self.features, original)

    def test_zero_strength_control(self):
        result = self.run_eval(0)
        self.assertEqual(result["prediction_flips"], 0)
        self.assertEqual(result["modified_rows"], 0)

    def test_confusion_and_denominator(self):
        result = self.run_eval()
        self.assertEqual(result["originally_detected_attacks"], 1)
        self.assertEqual(result["baseline_confusion_matrix"], [[1, 1], [1, 1]])
        self.assertEqual(result["newly_missed_attacks"], 0)
        self.assertEqual(result["prediction_flips"], 0)

    def test_no_initially_detected_attacks(self):
        result = evaluate_perturbation(ThresholdModel(), pd.DataFrame({"packet_size": [1.0]}), [1], "packet_size", 0.1)
        self.assertIsNone(result["conditional_evasion_rate"])

    def test_missing_feature(self):
        with self.assertRaisesRegex(ValueError, "missing"):
            evaluate_perturbation(ThresholdModel(), self.features, self.labels, "missing")

    def test_invalid_strength(self):
        for strength in [-0.1, 0.21, float("nan"), float("inf")]:
            with self.subTest(strength=strength), self.assertRaises(ValueError):
                self.run_eval(strength)

    def test_invalid_values(self):
        self.features.loc[0, "packet_size"] = -1
        with self.assertRaisesRegex(ValueError, "nonnegative"):
            self.run_eval()

    def test_invalid_labels(self):
        with self.assertRaisesRegex(ValueError, "binary"):
            evaluate_perturbation(ThresholdModel(), self.features, [1, 2, 0, 0], "packet_size")

    def test_transition_accounting(self):
        result = self.run_eval(0.1)
        self.assertEqual(sum(result["transitions"].values()), result["prediction_flips"])
        self.assertEqual(result["transitions"]["attack_1_to_0"], result["newly_missed_attacks"])

    def test_benign_false_positive_correction(self):
        result = evaluate_perturbation(
            ThresholdModel(),
            pd.DataFrame({"packet_size": [10.0, 9.0]}),
            [0, 1],
            "packet_size",
            0.1,
        )
        self.assertEqual(result["transitions"]["benign_1_to_0"], 0)
        self.assertEqual(sum(result["transitions"].values()), result["prediction_flips"])

    def test_positive_evasion_count(self):
        class ReverseModel:
            def predict(self, frame):
                return (frame["packet_size"].to_numpy() < 10).astype(int)
        result = evaluate_perturbation(ReverseModel(), pd.DataFrame({"packet_size": [9.5, 5.0]}), [1, 1], "packet_size", 0.1)
        self.assertEqual(result["originally_detected_attacks"], 2)
        self.assertEqual(result["newly_missed_attacks"], 1)
        self.assertEqual(result["conditional_evasion_rate"], 0.5)
        self.assertEqual(result["transitions"]["attack_1_to_0"], 1)


if __name__ == "__main__":
    unittest.main()
