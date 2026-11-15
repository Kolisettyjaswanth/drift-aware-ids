import unittest
from src.robustness.validate_existing_report import validate_report


class ReportValidationTests(unittest.TestCase):
    def report(self):
        baseline = [[10, 3], [7, 2]]
        rows = []
        for feature in ("Average Packet Size", "Packet Length Mean", "Packet Length Std"):
            for strength in (0.0, 0.05, 0.1):
                flips = 0 if strength == 0 else 2
                rows.append({
                    "feature": feature, "relative_strength": strength,
                    "baseline_confusion_matrix": baseline,
                    "perturbed_confusion_matrix": [[10 + flips, 3 - flips], [7, 2]],
                    "prediction_flips": flips, "newly_missed_attacks": 0,
                    "modified_rows": 0 if strength == 0 else 22,
                })
        return {"test_truncated": False, "test_rows": 22, "results": rows}

    def test_valid_aggregate_inference(self):
        output = validate_report(self.report())
        self.assertEqual(output["validation"], "passed")
        self.assertEqual(output["configurations"][1]["transitions_inferred_from_aggregates"]["benign_1_to_0"], 2)

    def test_offsetting_flips_rejected(self):
        report = self.report()
        report["results"][1]["prediction_flips"] = 4
        with self.assertRaisesRegex(ValueError, "Offsetting"):
            validate_report(report)

    def test_zero_control_rejected(self):
        report = self.report()
        report["results"][0]["modified_rows"] = 1
        with self.assertRaisesRegex(ValueError, "Zero-strength"):
            validate_report(report)

    def test_missing_configuration_rejected(self):
        report = self.report()
        report["results"].pop()
        with self.assertRaisesRegex(ValueError, "nine"):
            validate_report(report)


if __name__ == "__main__":
    unittest.main()
