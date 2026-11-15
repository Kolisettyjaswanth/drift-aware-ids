import unittest
import pandas as pd
from src.robustness.run_heldout_experiment import prepare_data, ALLOWED_FEATURES
from src.training.evaluate_heldout_capture import HOLDOUT


class HeldoutRunnerTests(unittest.TestCase):
    def setUp(self):
        values = {name: [1.0, 2.0] for name in ALLOWED_FEATURES}
        self.train = pd.DataFrame({**values, "Label": ["BENIGN", "PortScan"], "SourceFile": ["train.csv"] * 2})
        self.test = pd.DataFrame({**values, "Label": ["BENIGN", "PortScan"], "SourceFile": [HOLDOUT] * 2})

    def test_capture_separation_and_binary_labels(self):
        columns, train_y, test_y = prepare_data(self.train, self.test, set())
        self.assertEqual(list(train_y), [0, 1])
        self.assertEqual(list(test_y), [0, 1])
        self.assertEqual(columns, list(ALLOWED_FEATURES))

    def test_holdout_leakage_rejected(self):
        self.train.loc[0, "SourceFile"] = HOLDOUT
        with self.assertRaisesRegex(ValueError, "holdout"):
            prepare_data(self.train, self.test, set())

    def test_non_holdout_test_rejected(self):
        self.test.loc[0, "SourceFile"] = "other.csv"
        with self.assertRaisesRegex(ValueError, "holdout"):
            prepare_data(self.train, self.test, set())

    def test_missing_approved_feature_rejected(self):
        self.test = self.test.drop(columns=[ALLOWED_FEATURES[0]])
        with self.assertRaisesRegex(ValueError, "missing"):
            prepare_data(self.train, self.test, set())

    def test_non_numeric_feature_rejected(self):
        self.train[ALLOWED_FEATURES[0]] = ["a", "b"]
        with self.assertRaisesRegex(ValueError, "Non-numeric"):
            prepare_data(self.train, self.test, set())


if __name__ == "__main__":
    unittest.main()
