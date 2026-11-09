
from pathlib import Path
import json

import joblib
import pandas as pd

MODEL_DIR = Path("models")


def main():
    model_path = MODEL_DIR / "baseline_random_forest.joblib"
    features_path = MODEL_DIR / "baseline_features.json"
    metrics_path = MODEL_DIR / "baseline_metrics.json"

    for path in [model_path, features_path, metrics_path]:
        if not path.exists():
            raise FileNotFoundError(path)

    pipeline = joblib.load(model_path)

    features = json.loads(
        features_path.read_text(encoding="utf-8")
    )

    metrics = json.loads(
        metrics_path.read_text(encoding="utf-8")
    )

    classifier = pipeline.named_steps["classifier"]

    if len(features) != len(classifier.feature_importances_):
        raise ValueError("Feature count mismatch")

    importance = pd.DataFrame({
        "Feature": features,
        "Importance": classifier.feature_importances_,
    }).sort_values("Importance", ascending=False)

    print("=== MODEL VERIFICATION ===")
    print("Model:", type(classifier).__name__)
    print("Features:", len(features))
    print("Accuracy:", round(metrics["accuracy"] * 100, 4), "%")
    print("Attack recall:", round(metrics["recall"] * 100, 4), "%")
    print("Attack F1:", round(metrics["f1_score"] * 100, 4), "%")

    print("\n=== TOP 20 IMPORTANT FEATURES ===")
    print(importance.head(20).to_string(index=False))

    output = MODEL_DIR / "baseline_feature_importance.csv"
    importance.to_csv(output, index=False)

    print("\nSaved:", output)


if __name__ == "__main__":
    main()
