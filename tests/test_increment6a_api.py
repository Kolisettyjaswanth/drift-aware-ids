"""Isolated read-only API tests; no Kafka or Spark required."""
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from backend.app.main import app


class EvidenceApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = patch.dict(os.environ, {"IDS_EVIDENCE_ROOT": str(self.root)})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.client = TestClient(app)

    def test_health(self):
        response = self.client.get("/api/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    def test_only_runs_with_summary_are_listed(self):
        (self.root / "run-1").mkdir()
        (self.root / "run-1" / "summary.json").write_text('{"success": true}')
        (self.root / "run-2").mkdir()
        response = self.client.get("/api/runs")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["runs"], [
            {"run_id": "run-1", "summary_present": True}
        ])

    def test_limit_is_enforced(self):
        self.assertEqual(self.client.get("/api/runs?limit=0").status_code, 422)
        self.assertEqual(self.client.get("/api/runs?limit=101").status_code, 422)

    def test_missing_root_is_unavailable(self):
        with patch.dict(os.environ, {"IDS_EVIDENCE_ROOT": str(self.root / "missing")}):
            self.assertEqual(self.client.get("/api/runs").status_code, 503)

    def test_symlink_run_is_not_followed(self):
        outside = self.root / "actual"
        outside.mkdir()
        (outside / "summary.json").write_text("{}")
        (self.root / "linked").symlink_to(outside, target_is_directory=True)
        runs = self.client.get("/api/runs").json()["runs"]
        self.assertEqual([item["run_id"] for item in runs], ["actual"])

    def test_run_detail_valid(self):
        directory = self.root / "sample"
        directory.mkdir()
        (directory / "summary.json").write_text(
            '{"success":true,"accounting_complete":true,"errors":[]}'
        )
        response = self.client.get("/api/runs/sample")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["accounting_complete"])
        self.assertIsNone(response.json()["drift"])

    def test_run_detail_rejects_traversal(self):
        self.assertEqual(self.client.get("/api/runs/..").status_code, 404)
        self.assertEqual(self.client.get("/api/runs/%2E%2E").status_code, 400)

    def test_run_detail_rejects_malformed_summary(self):
        directory = self.root / "bad"
        directory.mkdir()
        (directory / "summary.json").write_text('{"success": "yes"}')
        self.assertEqual(self.client.get("/api/runs/bad").status_code, 422)

    def test_run_detail_rejects_oversized_summary(self):
        directory = self.root / "large"
        directory.mkdir()
        (directory / "summary.json").write_text("x" * (1024 * 1024 + 1))
        self.assertEqual(self.client.get("/api/runs/large").status_code, 422)

    def test_run_detail_rejects_symlink_summary(self):
        directory = self.root / "linked-summary"
        directory.mkdir()
        target = self.root / "real.json"
        target.write_text('{"success":true,"accounting_complete":true}')
        (directory / "summary.json").symlink_to(target)
        self.assertEqual(self.client.get("/api/runs/linked-summary").status_code, 404)


if __name__ == "__main__":
    unittest.main()
