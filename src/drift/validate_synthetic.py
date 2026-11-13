"""Predefined controlled validation. Run: python -m src.drift.validate_synthetic."""
import hashlib
import json
import platform
from pathlib import Path
import numpy as np
import pandas as pd
import scipy
from src.drift.ks import FEATURES, detect_window


def wilson(successes, n):
    z = 1.959963984540054
    p = successes/n
    d = 1+z*z/n
    center = (p+z*z/(2*n))/d
    half = z*np.sqrt(p*(1-p)/n+z*z/(4*n*n))/d
    return [float(center-half), float(center+half)]


def validate():
    reports = []
    digest = hashlib.sha256()
    for seed in range(5):
        rng = np.random.default_rng(seed)
        ref = pd.DataFrame(rng.normal(size=(10000, 10)), columns=FEATURES)
        counts = dict(no_shift=0, location_shift=0, scale_shift=0)
        for _ in range(40):
            window = pd.DataFrame(rng.normal(size=(1000, 10)), columns=FEATURES)
            digest.update(window.to_numpy().tobytes())
            for scenario in counts:
                changed = window.copy()
                if scenario == 'location_shift':
                    changed.iloc[:, :2] += 1.5
                elif scenario == 'scale_shift':
                    changed.iloc[:, :2] *= 4
                counts[scenario] += int(detect_window(ref, changed)['alert'])
        reports.append({'seed': seed, 'windows': 40, 'alerts': counts})
    summary = {}
    for scenario in counts:
        n = sum(r['alerts'][scenario] for r in reports)
        summary[scenario] = {'alerts': n, 'windows': 200, 'rate': n/200,
                             'wilson_95': wilson(n, 200)}
    passed = (summary['no_shift']['rate'] <= .05 and
              all(summary[s]['rate'] >= .90 for s in ('location_shift', 'scale_shift')))
    return {'passed': passed, 'seeds': reports, 'summary': summary,
            'settings': {'reference_size': 10000, 'window_size': 1000,
                         'alpha': .05, 'min_ks': .10, 'minimum_features': 2,
                         'shifted_features': FEATURES[:2]},
            'input_sha256': digest.hexdigest(),
            'versions': {'python': platform.python_version(), 'numpy': np.__version__,
                         'pandas': pd.__version__, 'scipy': scipy.__version__},
            'limitations': ['IID synthetic Gaussian validation only',
                           'Wilson intervals are descriptive: windows share references',
                           'Does not validate CIC-IDS2017 or deployment performance']}


if __name__ == '__main__':
    report = validate()
    out = Path('docs/validation/synthetic.json')
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, allow_nan=False)+'\n')
    print(json.dumps(report['summary'], indent=2))
    raise SystemExit(0 if report['passed'] else 1)
