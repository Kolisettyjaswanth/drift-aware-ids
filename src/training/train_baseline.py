
from pathlib import Path
import json

import joblib
import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report,
)
from sklearn.pipeline import Pipeline

DATA_DIR = Path("data/processed/ml")
MODEL_DIR = Path("models")
SEED = 42


def main():
    print("Loading training and testing datasets...")

    train = pd.read_csv(DATA_DIR / "train.csv")
    test = pd.read_csv(DATA_DIR / "test.csv")

    X_train = train.drop(columns=["Target"])
    y_train = train["Target"]

    X_test = test.drop(columns=["Target"])
    y_test = test["Target"]

    if X_train.columns.tolist() != X_test.columns.tolist():
        raise ValueError("Training and testing feature mismatch")

    features = X_train.columns.tolist()

    X_train = X_train.replace([np.inf, -np.inf], np.nan)
    X_test = X_test.replace([np.inf, -np.inf], np.nan)

    print("Training Random Forest...")

    pipeline = Pipeline([
        (
            "imputer",
            SimpleImputer(
                strategy="median",
                keep_empty_features=True,
            ),
        ),
        (
            "classifier",
            RandomForestClassifier(
                n_estimators=100,
                max_depth=20,
                min_samples_leaf=2,
                class_weight="balanced_subsample",
                random_state=SEED,
                n_jobs=2,
            ),
        ),
    ])

    pipeline.fit(X_train, y_train)

    print("Evaluating model...")

    predictions = pipeline.predict(X_test)

    metrics = {
        "accuracy": float(
            accuracy_score(y_test, predictions)
        ),
        "precision": float(
            precision_score(y_test, predictions, zero_division=0)
        ),
        "recall": float(
            recall_score(y_test, predictions, zero_division=0)
        ),
        "f1_score": float(
            f1_score(y_test, predictions, zero_division=0)
        ),
        "confusion_matrix": confusion_matrix(
            y_test,
            predictions,
            labels=[0, 1],
        ).tolist(),
        "feature_count": len(features),
        "model": "RandomForestClassifier",
        "evaluation_type": "stratified random development split",
    }

    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    joblib.dump(
        pipeline,
        MODEL_DIR / "baseline_random_forest.joblib",
    )

    (MODEL_DIR / "baseline_metrics.json").write_text(
        json.dumps(metrics, indent=2)
    )

    (MODEL_DIR / "baseline_features.json").write_text(
        json.dumps(features, indent=2)
    )

    print("\n=== MODEL EVALUATION ===")

    print(classification_report(
        y_test,
        predictions,
        target_names=["BENIGN", "ATTACK"],
        zero_division=0,
    ))

    print("Confusion matrix:")
    print(metrics["confusion_matrix"])

    print("\nSaved model and metrics in:", MODEL_DIR)


if __name__ == "__main__":
    main()
