
import json
from pathlib import Path

REPORT_PATH = Path("models/drift_friday_groups.json")
OUTPUT_PATH = Path("models/drift_alert_rule_comparison.json")


def main():
    report = json.loads(REPORT_PATH.read_text())

    benign_counts = [
        window["drifted_feature_count"]
        for window in report["benign"]["windows"]
    ]

    attack_counts = [
        window["drifted_feature_count"]
        for window in report["attack"]["windows"]
    ]

    results = []

    print("Minimum Features | Benign Flags | PortScan Flags")
    print("-" * 49)

    for minimum_features in range(1, 11):
        benign_flags = sum(
            count >= minimum_features
            for count in benign_counts
        )

        attack_flags = sum(
            count >= minimum_features
            for count in attack_counts
        )

        results.append({
            "minimum_drifted_features": minimum_features,
            "benign_flagged_windows": benign_flags,
            "portscan_flagged_windows": attack_flags,
            "benign_flag_rate": benign_flags / len(benign_counts),
            "portscan_flag_rate": attack_flags / len(attack_counts),
        })

        print(
            f"{minimum_features:^16} | "
            f"{benign_flags:^12} | "
            f"{attack_flags:^14}"
        )

    OUTPUT_PATH.write_text(
        json.dumps({
            "evaluation_type": "retrospective_alert_rule_comparison",
            "source_report": str(REPORT_PATH),
            "results": results,
        }, indent=2)
    )

    print("\nSaved:", OUTPUT_PATH)


if __name__ == "__main__":
    main()
