"""Isolated read-only API tests; no Kafka or Spark required."""
import os
import json
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

    def test_predictions_unverified_run_is_rejected(self):
        directory = self.root / "unverified"
        directory.mkdir()
        (directory / "summary.json").write_text(
            '{"success":true,"accounting_complete":true,"inference":null}'
        )
        self.assertEqual(
            self.client.get("/api/runs/unverified/predictions").status_code, 409
        )

    def test_predictions_incomplete_run_is_rejected(self):
        directory = self.root / "incomplete"
        directory.mkdir()
        (directory / "summary.json").write_text(
            '{"success":false,"accounting_complete":false,'
            '"inference":{"verified":true}}'
        )
        self.assertEqual(
            self.client.get("/api/runs/incomplete/predictions").status_code, 409
        )

    def test_predictions_pagination_limits(self):
        self.assertEqual(
            self.client.get("/api/runs/example/predictions?limit=101").status_code, 422
        )
        self.assertEqual(
            self.client.get("/api/runs/example/predictions?offset=-1").status_code, 422
        )

    def test_predictions_missing_evidence_is_rejected(self):
        directory = self.root / "missing-batches"
        directory.mkdir()
        (directory / "summary.json").write_text(
            '{"success":true,"accounting_complete":true,'
            '"inference":{"verified":true,"batches":1,'
            '"valid_records":1,"predictions":1}}'
        )
        self.assertEqual(
            self.client.get("/api/runs/missing-batches/predictions").status_code, 422
        )

    def _drift_fixture(self, *, mismatch=False, missing=False):
        directory = self.root / "drift-case"
        directory.mkdir()
        snapshot = "a" * 64
        reference = "b" * 64
        window = {"window_index": 0, "status": "evaluated",
                  "records": 1000, "alert": True, "source_capture": "PortScan"}
        drift = {"status": "evaluated", "success": True,
                 "evaluation_complete": True, "snapshot_id": snapshot,
                 "reference_sha256": reference, "evaluated_windows": 1,
                 "windows": [window], "errors": []}
        (directory / "summary.json").write_text(json.dumps(
            {"success": True, "accounting_complete": True, "drift": drift}))
        if not missing:
            evaluations = directory / "drift" / "evaluations"
            evaluations.mkdir(parents=True)
            report = {"snapshot_id": "c" * 64 if mismatch else snapshot,
                      "status": "evaluated", "success": True,
                      "evaluation_complete": True, "evaluated_windows": 1,
                      "reference": {"sha256": reference},
                      "windows": [{**window, "membership": [{"event_id": "hidden"}],
                                   "ks_results": {"alert": True, "features": {}}}]}
            (evaluations / (snapshot + ".json")).write_text(json.dumps(report))
        return directory

    def test_drift_disabled(self):
        directory = self.root / "drift-disabled"
        directory.mkdir()
        (directory / "summary.json").write_text(
            '{"success":true,"accounting_complete":true,"drift":null}'
        )
        response = self.client.get("/api/runs/drift-disabled/drift")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "disabled")

    def test_drift_authoritative_snapshot_compact(self):
        self._drift_fixture()
        response = self.client.get("/api/runs/drift-case/drift")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["evaluated_windows"], 1)
        self.assertIs(data["windows"][0]["alert"], True)
        self.assertIn("ks_results", data["windows"][0])
        self.assertNotIn("membership", data["windows"][0])

    def test_drift_missing_snapshot_rejected(self):
        self._drift_fixture(missing=True)
        self.assertEqual(self.client.get("/api/runs/drift-case/drift").status_code, 422)

    def test_drift_snapshot_mismatch_rejected(self):
        self._drift_fixture(mismatch=True)
        self.assertEqual(self.client.get("/api/runs/drift-case/drift").status_code, 422)

    def test_drift_failure_without_snapshot(self):
        directory = self.root / "drift-failure"
        directory.mkdir()
        (directory / "summary.json").write_text(json.dumps({
            "success": False, "accounting_complete": False,
            "drift": {"status": "monitoring_failure", "success": False,
                      "evaluation_complete": False, "evaluated_windows": 0,
                      "errors": ["partition mismatch"], "windows": []}}))
        response = self.client.get("/api/runs/drift-failure/drift")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "monitoring_failure")

    def test_drift_insufficient_data_without_snapshot(self):
        directory = self.root / "drift-partial"
        directory.mkdir()
        (directory / "summary.json").write_text(json.dumps({
            "success": False, "accounting_complete": False,
            "drift": {"status": "insufficient_data", "success": False,
                      "evaluation_complete": False, "evaluated_windows": 0,
                      "windows": [{"window_index": 0, "status": "insufficient_data",
                                   "records": 200, "alert": None}],
                      "errors": []}}))
        response = self.client.get("/api/runs/drift-partial/drift")
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["windows"][0]["alert"])

    def test_drift_malformed_snapshot_rejected(self):
        directory = self._drift_fixture()
        path = directory / "drift" / "evaluations" / (("a" * 64) + ".json")
        path.write_text("not-json")
        self.assertEqual(self.client.get("/api/runs/drift-case/drift").status_code, 422)


if __name__ == "__main__":
    unittest.main()
