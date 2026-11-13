
from pathlib import Path
import json

import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)

from src.training.sample_capture_data import (
    sample_training_data,
)


# ==================================================
# CONFIGURATION
# ==================================================

DATA = Path(
    "data/processed/capture_aware_cicids2017.csv"
)

AUDIT = Path(
    "data/processed/dataset_audit.json"
)

OUTPUT = Path(
    "models/heldout_portscan_metrics.json"
)

PREDICTION_OUTPUT = Path(
    "data/processed/drift/heldout_portscan_predictions.csv"
)

HOLDOUT = (
    "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv"
)

CHUNK_SIZE = 50_000
SEED = 42

DRIFT_FEATURES = [
    "Average Packet Size",
    "Bwd Packet Length Std",
    "Packet Length Variance",
    "Max Packet Length",
    "Bwd Packet Length Mean",
    "Packet Length Mean",
    "Destination Port",
    "Fwd Packet Length Max",
    "Total Length of Bwd Packets",
    "Packet Length Std",
]


# ==================================================
# MAIN FUNCTION
# ==================================================

def main():

    # STEP 1: Load dataset audit information

    audit = json.loads(AUDIT.read_text())

    excluded = set(
        audit["constant_columns"]
    )

    # STEP 2: Sample training data

    print("Sampling training data...")

    train = sample_training_data(HOLDOUT)

    # STEP 3: Load held-out testing data

    print("\nLoading held-out capture...")

    test_parts = []

    for chunk in pd.read_csv(
        DATA,
        chunksize=CHUNK_SIZE,
        low_memory=False,
    ):
        chunk.columns = chunk.columns.str.strip()

        test_chunk = chunk.loc[
            chunk["SourceFile"].eq(HOLDOUT)
        ].copy()

        if not test_chunk.empty:
            test_parts.append(test_chunk)

    if not test_parts:
        raise ValueError(
            f"No test records found for {HOLDOUT}"
        )

    test = pd.concat(
        test_parts,
        ignore_index=True,
    )

    # STEP 4: Verify capture separation

    assert not train["SourceFile"].eq(
        HOLDOUT
    ).any(), "Holdout leakage in training data!"

    assert test["SourceFile"].eq(
        HOLDOUT
    ).all(), "Unexpected capture in test data!"

    print("\n=== DATASET VERIFICATION ===")

    print(
        "Training records:",
        len(train)
    )

    print(
        "Testing records:",
        len(test)
    )

    print(
        "Training captures:",
        train["SourceFile"].nunique()
    )

    print(
        "Holdout present in training:",
        train["SourceFile"].eq(HOLDOUT).any()
    )

    # STEP 5: Identify original attack labels

    train_labels = sorted(
        train["Label"].astype(str).unique()
    )

    test_labels = sorted(
        test["Label"].astype(str).unique()
    )

    # STEP 6: Convert labels to binary targets
    # BENIGN = 0, ATTACK = 1

    train["Target"] = (
        train["Label"]
        .astype(str)
        .str.strip()
        .ne("BENIGN")
        .astype(int)
    )

    test["Target"] = (
        test["Label"]
        .astype(str)
        .str.strip()
        .ne("BENIGN")
        .astype(int)
    )

    # STEP 7: Select model features

    feature_columns = [
        col
        for col in train.columns
        if col not in excluded
        and col not in {
            "Label",
            "Target",
            "SourceFile",
        }
    ]

    missing_features = (
        set(feature_columns) - set(test.columns)
    )

    if missing_features:
        raise ValueError(
            f"Missing test features: {missing_features}"
        )

    missing_drift_features = (
        set(DRIFT_FEATURES) - set(feature_columns)
    )

    if missing_drift_features:
        raise ValueError(
            "Drift features missing from model: "
            f"{missing_drift_features}"
        )

    X_train = train[feature_columns]
    y_train = train["Target"]

    X_test = test[feature_columns]
    y_test = test["Target"]

    print(
        "\nNumber of model features:",
        len(feature_columns)
    )

    print(
        "Drift features available:",
        len(DRIFT_FEATURES)
    )

    # STEP 8: Build Random Forest pipeline

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

    # STEP 9: Train the classifier

    print(
        "\nTraining held-out evaluation model..."
    )

    pipeline.fit(
        X_train,
        y_train
    )

    # STEP 10: Predict held-out records

    print(
        "\nEvaluating held-out capture..."
    )

    predictions = pipeline.predict(
        X_test
    )

    # STEP 11: Export individual predictions
    # for later drift-versus-error analysis

    print(
        "\nExporting individual predictions..."
    )

    prediction_data = test[
        ["SourceFile", "Label"]
    ].copy()

    prediction_data["Actual"] = (
        y_test.to_numpy()
    )

    prediction_data["Predicted"] = (
        predictions
    )

    prediction_data["Correct"] = (
        prediction_data["Actual"]
        == prediction_data["Predicted"]
    ).astype(int)

    prediction_data = pd.concat(
        [
            prediction_data,
            test[DRIFT_FEATURES].copy(),
        ],
        axis=1,
    )

    PREDICTION_OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    prediction_data.to_csv(
        PREDICTION_OUTPUT,
        index=False,
    )

    print(
        "Held-out predictions saved to:",
        PREDICTION_OUTPUT
    )

    # STEP 12: Calculate evaluation metrics

    results = {
        "holdout_capture": HOLDOUT,

        "training_rows": int(len(train)),

        "testing_rows": int(len(test)),

        "training_labels": train_labels,

        "testing_labels": test_labels,

        "unseen_test_labels": sorted(
            set(test_labels) - set(train_labels)
        ),

        "model_features": len(feature_columns),

        "drift_features": DRIFT_FEATURES,

        "prediction_file": str(
            PREDICTION_OUTPUT
        ),

        "accuracy": float(
            accuracy_score(
                y_test,
                predictions
            )
        ),

        "precision": float(
            precision_score(
                y_test,
                predictions,
                zero_division=0
            )
        ),

        "recall": float(
            recall_score(
                y_test,
                predictions,
                zero_division=0
            )
        ),

        "f1_score": float(
            f1_score(
                y_test,
                predictions,
                zero_division=0
            )
        ),

        "confusion_matrix": (
            confusion_matrix(
                y_test,
                predictions,
                labels=[0, 1]
            ).tolist()
        ),

        "evaluation_type": "held-out capture",
    }

    # STEP 13: Save evaluation metrics

    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    OUTPUT.write_text(
        json.dumps(
            results,
            indent=2
        )
    )

    # STEP 14: Display final results

    print(
        "\n=== HELD-OUT CAPTURE EVALUATION ==="
    )

    print(
        "Training rows:",
        results["training_rows"]
    )

    print(
        "Testing rows:",
        results["testing_rows"]
    )

    print(
        "Unseen labels:",
        results["unseen_test_labels"]
    )

    print(
        "Accuracy:",
        round(
            results["accuracy"] * 100,
            4
        ),
        "%"
    )

    print(
        "Precision:",
        round(
            results["precision"] * 100,
            4
        ),
        "%"
    )

    print(
        "Recall:",
        round(
            results["recall"] * 100,
            4
        ),
        "%"
    )

    print(
        "F1:",
        round(
            results["f1_score"] * 100,
            4
        ),
        "%"
    )

    print(
        "Confusion matrix:",
        results["confusion_matrix"]
    )

    print(
        "\nMetrics saved to:",
        OUTPUT
    )

    print(
        "Predictions saved to:",
        PREDICTION_OUTPUT
    )


# ==================================================
# PROGRAM ENTRY POINT
# ==================================================

if __name__ == "__main__":
    main()
