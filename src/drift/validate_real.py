"""Paired same-capture benign validation; never substitutes fixtures for real results."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import subprocess

import numpy as np
import pandas as pd
from src.drift.ks import FEATURES, detect_window
from src.drift.validate_synthetic import wilson

SEEDS = (0, 1, 2, 3, 4)
SHIFT_FEATURES = ('Average Packet Size', 'Packet Length Mean')
SETTINGS = dict(features=FEATURES, minimum_features=2, alpha=.05,
                min_ks=.10, window_size=1000)
REFERENCE_SIZE = 10000
WINDOWS = 40


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def frame_hash(frame):
    """Canonical ordered CSV serialization, including row identities and labels."""
    return hashlib.sha256(frame.to_csv(index=True, float_format='%.17g',
                                       lineterminator='\n').encode()).hexdigest()


def load_capture(path):
    before = file_hash(path)
    frame = pd.read_csv(path, encoding='utf-8-sig', low_memory=False)
    frame.columns = frame.columns.str.strip()
    if frame.columns.duplicated().any():
        raise ValueError('Duplicate columns after whitespace normalization')
    required = FEATURES + ['Label']
    if set(required) - set(frame.columns):
        raise ValueError('Missing monitored features or Label')
    # A raw, single-capture CSV is required; avoid silently combining captures.
    if 'SourceFile' in frame and frame.SourceFile.nunique(dropna=False) != 1:
        raise ValueError('Input must contain exactly one capture')
    frame.index.name = 'source_row'
    labels = frame.Label.astype('string').str.strip()
    if labels.isna().any() or not labels.eq('BENIGN').all():
        raise ValueError('Input must be an entirely BENIGN capture')
    frame = frame[required].copy()
    frame['Label'] = labels
    for feature in FEATURES:
        frame[feature] = pd.to_numeric(frame[feature], errors='coerce')
    finite = np.isfinite(frame[FEATURES].to_numpy(dtype=float)).all(axis=1)
    # One common eligibility filter before sampling, never scenario-specific.
    eligible = frame.loc[finite].copy()
    after = file_hash(path)
    if before != after:
        raise ValueError('Input changed while loading')
    return eligible, {'path': str(Path(path).resolve()), 'file_sha256': before,
                      'input_rows': len(frame), 'excluded_nonfinite_rows': int((~finite).sum()),
                      'eligible_rows': len(eligible), 'row_identity': 'zero-based parsed CSV record index',
                      'eligible_sha256': frame_hash(eligible)}


def split_samples(frame, seed, reference_size=REFERENCE_SIZE, windows=WINDOWS,
                  window_size=1000):
    if not frame.index.is_unique:
        raise ValueError('Row identities must be unique')
    if reference_size < 100 or windows < 1 or window_size < 100:
        raise ValueError('Invalid sample sizes')
    required = reference_size + windows*window_size
    if len(frame) < required:
        raise ValueError(f'Need {required} eligible rows, found {len(frame)}')
    positions = np.random.default_rng(seed).choice(len(frame), required, replace=False)
    ref = frame.iloc[positions[:reference_size]].copy()
    evaluation = frame.iloc[positions[reference_size:]].copy()
    if not ref.index.intersection(evaluation.index).empty:
        raise ValueError('Reference/evaluation overlap')
    return ref, evaluation


def paired_scenarios(reference, window):
    scenarios = {'unchanged': window.copy(deep=True)}
    location, scale = window.copy(deep=True), window.copy(deep=True)
    offsets = {}
    for feature in SHIFT_FEATURES:
        # Fixed formula based exclusively on reference; no test-result tuning.
        offset = 5 * max(float(reference[feature].std(ddof=0)), 1.)
        offsets[feature] = offset
        location[feature] = location[feature] + offset
        scale[feature] = scale[feature] * 10.
    scenarios.update(location_shift=location, scale_shift=scale)
    for copy in scenarios.values():
        if not copy.index.equals(window.index) or not copy.Label.equals(window.Label):
            raise ValueError('Paired identities or labels changed')
        unchanged_columns = [c for c in window if c not in SHIFT_FEATURES]
        if not copy[unchanged_columns].equals(window[unchanged_columns]):
            raise ValueError('Unspecified columns changed')
    return scenarios, offsets


def acceptance_passed(aggregate):
    return aggregate['unchanged']['rate'] <= .05 and all(
        aggregate[n]['rate'] >= .90 for n in ('location_shift', 'scale_shift'))


def run_validation(frame, provenance, *, seeds=SEEDS, reference_size=REFERENCE_SIZE,
                   windows=WINDOWS, settings=None):
    settings = dict(SETTINGS if settings is None else settings)
    if len(seeds) < 5 or len(set(seeds)) != len(seeds):
        raise ValueError('At least five unique seeds required')
    reports = []
    for seed in seeds:
        ref, evaluation = split_samples(frame, seed, reference_size, windows,
                                        settings['window_size'])
        counts = dict(unchanged=0, location_shift=0, scale_shift=0)
        details = []
        for i in range(windows):
            window = evaluation.iloc[i*settings['window_size']:(i+1)*settings['window_size']]
            scenarios, offsets = paired_scenarios(ref, window)
            outcomes = {name: detect_window(ref, sample, **settings)
                        for name, sample in scenarios.items()}
            for name, outcome in outcomes.items():
                counts[name] += int(outcome['alert'])
            details.append({'window': i, 'row_ids': window.index.tolist(),
                            'labels_sha256': frame_hash(window[['Label']]),
                            'attack_fraction': float(window.Label.ne('BENIGN').mean()),
                            'scenario_sha256': {n: frame_hash(s) for n,s in scenarios.items()},
                            'results': outcomes})
        reports.append({'seed': seed, 'reference_row_ids': ref.index.tolist(),
                        'reference_sha256': frame_hash(ref),
                        'evaluation_sha256': frame_hash(evaluation),
                        'location_offsets': offsets, 'alerts': counts,
                        'rates': {name: count/windows for name,count in counts.items()},
                        'windows': details})
    total = len(seeds)*windows
    aggregate = {}
    for name in counts:
        count = sum(r['alerts'][name] for r in reports)
        aggregate[name] = {'alerts': count, 'windows': total, 'rate': count/total,
                           'wilson_95_descriptive': wilson(count, total)}
    passed = acceptance_passed(aggregate)
    return {'passed': passed, 'provenance': provenance, 'settings': settings,
            'reference_size': reference_size, 'windows_per_seed': windows,
            'shift_features': SHIFT_FEATURES, 'scale_multiplier': 10,
            'location_formula': 'x + 5 * max(reference population std, 1)',
            'acceptance': {'unchanged_max': .05, 'each_shift_min': .90},
            'aggregate': aggregate, 'seeds': reports,
            'limitations': ['Same-capture random windows are not chronological deployment validation',
                           'Rows are disjoint by identity, not necessarily by duplicate feature values',
                           'Seeds may reuse rows; shared references and repeated seeds induce dependence',
                           'Wilson intervals are descriptive, not independent-trial guarantees',
                           'Benign-only prevalence is zero; this does not validate mixed attack traffic',
                           'Artificial marginal shifts may be physically inconsistent network flows',
                           'Scaling zero-heavy features may produce little change; failure is reported, not tuned away']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True,
                        help='Single entirely benign raw capture CSV (Monday recommended)')
    parser.add_argument('--output', type=Path, default=Path('models/controlled_real_validation.json'))
    args = parser.parse_args()
    frame, provenance = load_capture(args.input)
    report = run_validation(frame, provenance)
    report['versions'] = {'python': platform.python_version(), **{
        name: importlib.metadata.version(name) for name in ('numpy', 'pandas', 'scipy')}}
    report['validator_sha256'] = file_hash(__file__)
    try:
        report['source_commit'] = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], text=True, stderr=subprocess.DEVNULL).strip()
        report['worktree_dirty'] = bool(subprocess.check_output(
            ['git', 'status', '--porcelain'], text=True).strip())
    except (OSError, subprocess.CalledProcessError):
        report['source_commit'] = None
        report['worktree_dirty'] = None
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False)+'\n')
    print(json.dumps({'passed': report['passed'], 'aggregate': report['aggregate'],
                      'output': str(args.output)}, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
