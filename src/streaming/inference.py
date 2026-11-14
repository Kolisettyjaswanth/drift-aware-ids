"""Random Forest inference for validated schema-v1 network-flow events."""

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


MODEL_PATH = Path("models/baseline_random_forest.joblib")
FEATURES_PATH = Path("models/baseline_features.json")

LABELS = {
    0: "BENIGN",
    1: "ATTACK",
}


class StreamingInference:
    """Load the trained pipeline once and predict validated event batches."""

    def __init__(self, model_path=MODEL_PATH, features_path=FEATURES_PATH):
        self.model = joblib.load(model_path)
        self.features = json.loads(Path(features_path).read_text())

        if list(self.model.feature_names_in_) != self.features:
            raise ValueError("Model feature order does not match metadata")

        if list(self.model.classes_) != [0, 1]:
            raise ValueError("Unexpected model classes")

    def predict_records(self, records):
        """Return predictions while preserving event and Kafka identities."""
        if not records:
            return []

        for record in records:
            if not record.get("valid"):
                raise ValueError("Inference requires validated records")

        rows = []

        for record in records:
            features = record["event"]["features"]

            missing = [
                name for name in self.features
                if name not in features
            ]

            if missing:
                raise ValueError(
                    f"Missing required model features: {missing}"
                )

            rows.append({
                name: features[name]
                for name in self.features
            })

        X = pd.DataFrame(rows, columns=self.features)
        X = X.apply(pd.to_numeric, errors="coerce")
        X = X.replace([np.inf, -np.inf], np.nan)

        predictions = np.asarray(self.model.predict(X))
        probabilities = np.asarray(self.model.predict_proba(X))

        if predictions.shape != (len(records),):
            raise ValueError("Unexpected prediction output shape")

        if probabilities.shape != (len(records), 2):
            raise ValueError("Unexpected probability output shape")

        if not np.isfinite(probabilities).all():
            raise ValueError("Non-finite model probabilities")

        if np.any(probabilities < 0) or np.any(probabilities > 1):
            raise ValueError("Model probabilities outside [0, 1]")

        if not np.allclose(probabilities.sum(axis=1), 1.0):
            raise ValueError("Model probabilities do not sum to 1")

        if not np.isin(predictions, [0, 1]).all():
            raise ValueError("Unexpected prediction class")

        results = []

        for record, prediction, probability in zip(
            records, predictions, probabilities
        ):
            event = record["event"]
            prediction = int(prediction)

            results.append({
                "event_id": event["event_id"],
                "replay_id": event["replay_id"],
                "source_row": event["source_row"],
                "kafka": record["kafka"],
                "prediction": prediction,
                "prediction_label": LABELS[prediction],
                "probability_benign": float(probability[0]),
                "probability_attack": float(probability[1]),
            })

        return results
