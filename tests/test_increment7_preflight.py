"""Unit tests for bounded, read-only CSV preflight."""
import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.preflight_replay import preflight


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.manifest = self.root / "features.json"
        self.manifest.write_text(json.dumps(["a", "b"]))
        self.source = self.root / "source.csv"

    def write_csv(self, headers, rows):
        with self.source.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(headers)
            writer.writerows(rows)

    def test_valid_bounded_source(self):
        self.write_csv(["a", "b", "Label", "SourceFile", "extra"], [["1", "2", "BENIGN", "capture", "3"]])
        result = preflight(self.source, self.manifest, max_rows=1)
        self.assertEqual(result["rows"], 1)
        self.assertEqual(result["extra_columns"], ["extra"])

    def test_missing_feature(self):
        self.write_csv(["a", "Label"], [["1", "BENIGN"]])
        with self.assertRaisesRegex(ValueError, "Missing required"):
            preflight(self.source, self.manifest)

    def test_invalid_numeric(self):
        self.write_csv(["a", "b", "Label"], [["1", "oops", "BENIGN"]])
        with self.assertRaisesRegex(ValueError, "missing/nonfinite/non-numeric"):
            preflight(self.source, self.manifest)

    def test_oversized_source(self):
        self.write_csv(["a", "b", "Label"], [["1", "2", "BENIGN"], ["3", "4", "BENIGN"]])
        with self.assertRaisesRegex(ValueError, "exceeds max_rows"):
            preflight(self.source, self.manifest, max_rows=1)

    def test_duplicate_header(self):
        self.write_csv(["a", "a", "b", "Label"], [["1", "2", "3", "BENIGN"]])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            preflight(self.source, self.manifest)

    def test_reject_training_target(self):
        self.write_csv(["a", "b", "Label", "Target"], [["1", "2", "BENIGN", "0"]])
        with self.assertRaisesRegex(ValueError, "rejects Target"):
            preflight(self.source, self.manifest)

    def test_empty_rows(self):
        self.write_csv(["a", "b", "Label"], [])
        with self.assertRaisesRegex(ValueError, "no data rows"):
            preflight(self.source, self.manifest)

    def test_manifest_invalid(self):
        self.manifest.write_text(json.dumps(["a", "a"]))
        self.write_csv(["a", "b", "Label"], [["1", "2", "BENIGN"]])
        with self.assertRaisesRegex(ValueError, "manifest"):
            preflight(self.source, self.manifest)


if __name__ == "__main__":
    unittest.main()
