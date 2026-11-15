import json
import tempfile
import unittest
from pathlib import Path
import pandas as pd
from src.drift.increment4_provenance import validate_provenance


class ProvenanceTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        self.manifest = base / "manifest.json"
        self.reference = base / "reference.csv"
        self.groups = {"groups": {
            "reference": {"captures": {"a.csv": 2, "b.csv": 1}, "total_rows": 3},
            "calibration": {"captures": {"c.csv": 2}, "total_rows": 2},
            "evaluation_benign": {"captures": {"holdout.csv": 1}, "total_rows": 1}}}
        self.frame = pd.DataFrame({"SourceFile": ["a.csv", "a.csv", "b.csv"], "Label": ["BENIGN"] * 3})
        self.save()

    def save(self):
        self.manifest.write_text(json.dumps(self.groups))
        self.frame.to_csv(self.reference, index=False)

    def check(self):
        return validate_provenance(self.manifest, self.reference, "holdout.csv")

    def test_valid(self):
        self.assertTrue(self.check()["passed"])

    def test_nonbenign(self):
        self.frame.loc[0, "Label"] = "PortScan"
        self.save()
        with self.assertRaisesRegex(ValueError, "BENIGN"):
            self.check()

    def test_mismatched_capture(self):
        self.frame.loc[0, "SourceFile"] = "unknown.csv"
        self.save()
        with self.assertRaisesRegex(ValueError, "capture"):
            self.check()

    def test_capture_overlap(self):
        self.groups["groups"]["calibration"]["captures"]["a.csv"] = 1
        self.save()
        with self.assertRaisesRegex(ValueError, "overlap"):
            self.check()

    def test_holdout_assignment(self):
        self.groups["groups"]["evaluation_benign"]["captures"] = {"other.csv": 1}
        self.save()
        with self.assertRaisesRegex(ValueError, "Holdout"):
            self.check()

    def test_capture_counts(self):
        self.groups["groups"]["reference"]["captures"]["a.csv"] = 1
        self.save()
        with self.assertRaisesRegex(ValueError, "counts"):
            self.check()

    def test_missing_reference(self):
        self.reference.unlink()
        with self.assertRaisesRegex(ValueError, "Missing"):
            self.check()


if __name__ == "__main__":
    unittest.main()
