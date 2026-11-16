"""Increment 6B verified classification aggregate acceptance tests."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from backend.app.prediction_evidence import read_prediction_counts


class PredictionCountsTests(unittest.TestCase):
    def test_full_run_counts_not_just_first_page(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "run"
            (run / "predictions").mkdir(parents=True)
            (run / "predictions" / "batch-000000.jsonl").write_text(
                '{"prediction":0}\n{"prediction":1}\n{"prediction":1}\n{"prediction":0}\n',
                encoding="utf-8",
            )
            with patch("backend.app.prediction_evidence.read_predictions",
                       return_value={"total": 4, "verified": True}):
                result = read_prediction_counts(root, "run")
            self.assertEqual(result["predicted_normal"], 2)
            self.assertEqual(result["predicted_attack"], 2)
            self.assertEqual(result["total"], 4)

    def test_rejects_mismatched_total(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "run"
            (run / "predictions").mkdir(parents=True)
            (run / "predictions" / "batch-000000.jsonl").write_text(
                '{"prediction":1}\n', encoding="utf-8")
            with patch("backend.app.prediction_evidence.read_predictions",
                       return_value={"total": 2, "verified": True}):
                with self.assertRaises(HTTPException) as ctx:
                    read_prediction_counts(root, "run")
            self.assertEqual(ctx.exception.status_code, 422)

    def test_rejects_invalid_prediction_label(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "run"
            (run / "predictions").mkdir(parents=True)
            (run / "predictions" / "batch-000000.jsonl").write_text(
                '{"prediction":true}\n', encoding="utf-8")
            with patch("backend.app.prediction_evidence.read_predictions",
                       return_value={"total": 1, "verified": True}):
                with self.assertRaises(HTTPException) as ctx:
                    read_prediction_counts(root, "run")
            self.assertEqual(ctx.exception.status_code, 422)


if __name__ == "__main__":
    unittest.main()
