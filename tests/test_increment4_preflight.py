import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.drift.analyze_drift_vs_errors import FEATURES
from src.drift.increment4_preflight import inspect


class Increment4PreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = Path(self.tmp.name) / "reference.csv"
        self.predictions = Path(self.tmp.name) / "predictions.csv"
        pd.DataFrame({name: [float(i) for i in range(100)] for name in FEATURES}).to_csv(self.reference, index=False)
        data = {name: [float(i % 100) for i in range(4000)] for name in FEATURES}
        data.update({"Actual": [0] * 4000, "Predicted": [0] * 4000,
                     "Correct": [1] * 4000, "Label": ["BENIGN"] * 4000,
                     "SourceFile": ["capture.csv"] * 4000})
        self.frame = pd.DataFrame(data)
        self.save()

    def save(self):
        self.frame.to_csv(self.predictions, index=False)

    def test_valid_inputs(self):
        report = inspect(self.reference, self.predictions)
        self.assertEqual(report["complete_windows"], 4)
        self.assertEqual(report["excluded_trailing_records"], 0)
        self.assertEqual(len(report["reference"]["sha256"]), 64)

    def test_inconsistent_correct_rejected(self):
        self.frame.loc[0, "Correct"] = 0
        self.save()
        with self.assertRaisesRegex(ValueError, "Correct disagrees"):
            inspect(self.reference, self.predictions)

    def test_mixed_captures_rejected(self):
        self.frame.loc[0, "SourceFile"] = "other.csv"
        self.save()
        with self.assertRaisesRegex(ValueError, "one named capture"):
            inspect(self.reference, self.predictions)

    def test_missing_feature_rejected(self):
        self.frame.drop(columns=[FEATURES[0]]).to_csv(self.predictions, index=False)
        with self.assertRaisesRegex(ValueError, "missing features"):
            inspect(self.reference, self.predictions)

    def test_nonfinite_rejected(self):
        self.frame.loc[0, FEATURES[0]] = float("inf")
        self.save()
        with self.assertRaisesRegex(ValueError, "nonfinite"):
            inspect(self.reference, self.predictions)

    def test_insufficient_windows_rejected(self):
        self.frame.iloc[:3000].to_csv(self.predictions, index=False)
        with self.assertRaisesRegex(ValueError, "four complete"):
            inspect(self.reference, self.predictions)


if __name__ == "__main__":
    unittest.main()
