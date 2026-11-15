"""Validate an existing Increment 5 report without rerunning model training.

Aggregate transition directions are inferred only when the reported total
flips equal the minimum flips required by the two confusion matrices.
Otherwise the validator fails closed: aggregate matrices alone cannot
identify offsetting bidirectional changes.
"""
import argparse
import json
from pathlib import Path


def validate_report(report):
    results = report.get("results")
    if report.get("test_truncated") is not False or not isinstance(results, list) or len(results) != 9:
        raise ValueError("Expected nine full-capture configurations")
    expected_features = {"Average Packet Size", "Packet Length Mean", "Packet Length Std"}
    expected_strengths = {0.0, 0.05, 0.1}
    seen = set()
    baseline = results[0]["baseline_confusion_matrix"]
    if len(baseline) != 2 or any(len(row) != 2 for row in baseline):
        raise ValueError("Invalid baseline confusion matrix")
    n = sum(map(sum, baseline))
    if n != report.get("test_rows"):
        raise ValueError("Baseline row count mismatch")
    inferred = []
    for result in results:
        key = (result["feature"], result["relative_strength"])
        if key in seen or key[0] not in expected_features or key[1] not in expected_strengths:
            raise ValueError("Duplicate or unexpected configuration")
        seen.add(key)
        if result["baseline_confusion_matrix"] != baseline:
            raise ValueError("Baseline changed between configurations")
        after = result["perturbed_confusion_matrix"]
        if len(after) != 2 or any(len(row) != 2 for row in after):
            raise ValueError("Invalid perturbed confusion matrix")
        if any(not isinstance(v, int) or v < 0 for row in after for v in row):
            raise ValueError("Invalid confusion counts")
        if any(sum(after[i]) != sum(baseline[i]) for i in (0, 1)):
            raise ValueError("Actual-class row totals changed")
        deltas = [after[i][1] - baseline[i][1] for i in (0, 1)]
        minimum_flips = sum(abs(x) for x in deltas)
        if result["prediction_flips"] != minimum_flips:
            raise ValueError("Offsetting transitions possible; cannot infer individual directions")
        transitions = {
            "benign_0_to_1": max(deltas[0], 0),
            "benign_1_to_0": max(-deltas[0], 0),
            "attack_0_to_1": max(deltas[1], 0),
            "attack_1_to_0": max(-deltas[1], 0),
        }
        if transitions["attack_1_to_0"] != result["newly_missed_attacks"]:
            raise ValueError("Newly missed count mismatch")
        if key[1] == 0 and (minimum_flips or result["modified_rows"]):
            raise ValueError("Zero-strength control changed")
        inferred.append({"feature": key[0], "relative_strength": key[1],
                         "transitions_inferred_from_aggregates": transitions})
    if seen != {(f, s) for f in expected_features for s in expected_strengths}:
        raise ValueError("Missing configuration")
    return {"validation": "passed", "source": "aggregate matrices and reported flip counts",
            "inference_limit": "Valid only because total flips equal minimum required matrix changes",
            "test_rows": n, "configurations": inferred}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, default=Path("models/increment5_robustness.json"))
    parser.add_argument("--output", type=Path, default=Path("models/increment5_transition_validation.json"))
    args = parser.parse_args()
    result = validate_report(json.loads(args.report.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
