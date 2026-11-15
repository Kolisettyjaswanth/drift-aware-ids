"""Deterministic, bounded offline feature-perturbation evaluation.

This module does not create traffic or modify the fitted classifier.
"""
import hashlib
import json

import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix


def evaluate_perturbation(model, features, labels, feature_name, relative_strength=0.05):
    """Compare predictions of one already-fitted classifier before and after a bounded edit.

    Only nonnegative numeric features are supported. Perturbation increases the
    selected feature by relative_strength, with a unit floor for zero values.
    """
    if not isinstance(features, pd.DataFrame) or features.empty:
        raise ValueError("features must be a nonempty DataFrame")
    if feature_name not in features.columns:
        raise ValueError("Perturbed feature is missing")
    if isinstance(relative_strength, bool) or not isinstance(relative_strength, (float, int)):
        raise ValueError("Strength must be numeric")
    if not np.isfinite(relative_strength) or not 0 <= relative_strength <= 0.20:
        raise ValueError("Strength must be finite and between 0 and 0.20")
    target = np.asarray(labels)
    if target.ndim != 1 or len(target) != len(features) or not np.isin(target, [0, 1]).all():
        raise ValueError("Labels must be binary and aligned with features")
    values = features[feature_name]
    if not pd.api.types.is_numeric_dtype(values):
        raise ValueError("Perturbed feature must be numeric")
    numeric = values.to_numpy(dtype=float)
    if not np.isfinite(numeric).all() or (numeric < 0).any():
        raise ValueError("Perturbed feature must be finite and nonnegative")
    clean = np.asarray(model.predict(features))
    if clean.shape != target.shape or not np.isin(clean, [0, 1]).all():
        raise ValueError("Model predictions must be aligned binary labels")
    changed = features.copy(deep=True)
    changed[feature_name] = numeric + relative_strength * np.maximum(numeric, 1.0)
    altered = np.asarray(model.predict(changed))
    if altered.shape != target.shape or not np.isin(altered, [0, 1]).all():
        raise ValueError("Altered predictions must be aligned binary labels")
    initially_detected = (target == 1) & (clean == 1)
    newly_missed = initially_detected & (altered == 0)
    count = int(initially_detected.sum())
    positives = int((target == 1).sum())
    negatives = int((target == 0).sum())
    changed_rows = int((changed[feature_name].to_numpy() != numeric).sum())
    return {
        "feature": feature_name,
        "relative_strength": float(relative_strength),
        "rows": int(len(target)),
        "modified_rows": changed_rows,
        "baseline_confusion_matrix": confusion_matrix(target, clean, labels=[0, 1]).tolist(),
        "perturbed_confusion_matrix": confusion_matrix(target, altered, labels=[0, 1]).tolist(),
        "baseline_attack_recall": float(initially_detected.sum() / positives) if positives else None,
        "perturbed_attack_recall": float(((target == 1) & (altered == 1)).sum() / positives) if positives else None,
        "baseline_benign_fpr": float(((target == 0) & (clean == 1)).sum() / negatives) if negatives else None,
        "perturbed_benign_fpr": float(((target == 0) & (altered == 1)).sum() / negatives) if negatives else None,
        "originally_detected_attacks": count,
        "newly_missed_attacks": int(newly_missed.sum()),
        "conditional_evasion_rate": float(newly_missed.sum() / count) if count else None,
        "prediction_flips": int((clean != altered).sum()),
        "limitations": "Synthetic independent tabular feature edit; not proven packet/flow feasible",
    }


def save_report(report, path):
    from pathlib import Path
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
