"""Descriptive Pearson and linear prevalence-adjusted correlations."""
import numpy as np


def correlations(ks, errors, prevalence):
    a = np.asarray([ks, errors, prevalence], dtype=float)
    if a.ndim != 2 or a.shape[0] != 3 or a.shape[1] < 4:
        raise ValueError('Need at least four aligned windows')
    if not np.isfinite(a).all() or ((a < 0) | (a > 1)).any():
        raise ValueError('Expected finite rates/statistics in [0, 1]')
    def pearson(x, y):
        if np.ptp(x) < 1e-12 or np.ptp(y) < 1e-12:
            return None
        return float(np.corrcoef(x, y)[0, 1])
    design = np.column_stack([np.ones(a.shape[1]), a[2]])
    residuals = [x - design @ np.linalg.lstsq(design, x, rcond=None)[0]
                 for x in a[:2]]
    return {'raw': pearson(*a[:2]), 'prevalence_adjusted': pearson(*residuals),
            'windows': a.shape[1], 'method': 'Pearson; OLS residuals with intercept',
            'limitation': 'Descriptive linear adjustment; not causal evidence'}
