"""Offline worker reporting tests for optional inference."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.streaming.spark_consumer import worker
from src.streaming.validation import SPARK_VERSION


class InferenceWorkerTests(unittest.TestCase):

    def test_verification_failure_cannot_report_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            config = {
                "run_dir": str(root),
                "replay_id": "test-replay",
                "expected_count": 1,
                "topic": "test-topic",
                "max_records": 10,
                "enable_inference": True,
            }

            spark = MagicMock()
            spark.version = SPARK_VERSION
            spark.sparkContext._jvm.scala.util.Properties.versionNumberString.return_value = "2.12.18"

            successful_accounting = {
                "success": True,
                "valid_count": 1,
            }

            with (
                patch("pyspark.__version__", SPARK_VERSION),
                patch("pyspark.sql.SparkSession.builder") as builder,
                patch(
                    "src.streaming.spark_consumer.execute_query"
                ),
                patch(
                    "src.streaming.spark_consumer.read_records",
                    return_value=iter([]),
                ),
                patch(
                    "src.streaming.spark_consumer.summarize",
                    return_value=successful_accounting,
                ),
                patch(
                    "src.streaming.verify_inference.verify_inference",
                    side_effect=ValueError(
                        "Simulated inference verification failure"
                    ),
                ),
            ):
                builder.master.return_value = builder
                builder.appName.return_value = builder
                builder.config.return_value = builder
                builder.getOrCreate.return_value = spark

                result = worker(config)

            self.assertEqual(result, 1)

            report = json.loads(
                (root / "summary.json").read_text()
            )

            self.assertFalse(report["success"])
            self.assertTrue(
                any(
                    "Simulated inference verification failure" in error
                    for error in report["errors"]
                ),
                f"Unexpected worker errors: {report['errors']}",
            )

            spark.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
