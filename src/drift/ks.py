"""Shared KS feature comparison; legacy cleaning is opt-in."""
import numpy as np
from scipy.stats import ks_2samp

FEATURES = [
    'Average Packet Size', 'Bwd Packet Length Std', 'Packet Length Variance',
    'Max Packet Length', 'Bwd Packet Length Mean', 'Packet Length Mean',
    'Destination Port', 'Fwd Packet Length Max', 'Total Length of Bwd Packets',
    'Packet Length Std',
]


def compare_feature(reference, current, *, alpha=.005, min_ks=.10,
                    min_samples=100, invalid='raise', method='asymp'):
    if not 0 < alpha < 1 or not 0 <= min_ks <= 1:
        raise ValueError('Invalid thresholds')
    if invalid not in {'raise', 'drop'} or min_samples < 2:
        raise ValueError('Invalid input policy')
    values = []
    for source in (reference, current):
        a = np.asarray(source, dtype=float)
        if a.ndim != 1:
            raise ValueError('Expected one-dimensional numeric values')
        if invalid == 'drop':
            a = a[np.isfinite(a)]
        elif not np.isfinite(a).all():
            raise ValueError('Nonfinite feature values')
        if len(a) < min_samples:
            raise ValueError('Insufficient feature samples')
        values.append(a)
    statistic, p_value = ks_2samp(*values, method=method)
    return {'ks_statistic': float(statistic), 'p_value': float(p_value),
            'drift_detected': feature_alert(statistic, p_value, alpha=alpha, min_ks=min_ks)}


def detect_window(reference, current, *, features=FEATURES, minimum_features=2,
                  alpha=.05, min_ks=.10, window_size=1000):
    features = tuple(features)
    if not features or len(set(features)) != len(features):
        raise ValueError('Features must be unique and nonempty')
    if not 1 <= minimum_features <= len(features) or window_size < 100:
        raise ValueError('Invalid window settings')
    if len(current) != window_size:
        raise ValueError('Incomplete or oversized window')
    if set(features) - set(reference.columns) or set(features) - set(current.columns):
        raise ValueError('Missing feature columns')
    results = {f: compare_feature(reference[f], current[f], alpha=alpha/len(features),
                                  min_ks=min_ks) for f in features}
    drifted = [f for f, r in results.items() if r['drift_detected']]
    return {'features': results, 'drifted_features': drifted,
            'drifted_feature_count': len(drifted),
            'alert': len(drifted) >= minimum_features}


def legacy_ks_2samp(reference, current, method='auto'):
    """Compatibility adapter: retain historical SciPy method and caller policies.

    Existing experiments still own their cleaning, minimum sample checks and
    one/two-feature window rules. All KS statistics use the shared implementation.
    """
    result = compare_feature(reference, current, min_samples=2, method=method)
    return result['ks_statistic'], result['p_value']


def feature_alert(statistic, p_value, *, alpha, min_ks):
    """Both statistical significance and effect size are required."""
    return bool(p_value < alpha and statistic >= min_ks)
