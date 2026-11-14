"""Execute the existing KS detector and persist immutable snapshot evidence.

No Spark wiring or alternate statistical detector lives here.
"""
from copy import deepcopy
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import re
import tempfile

import numpy as np
import pandas as pd
from src.drift import ks
from src.streaming.drift_reference import DriftReference, MAX_REFERENCE_ROWS
from src.streaming.drift_windows import WindowPlan, DETECTOR_SETTINGS, WINDOW_SIZE


class EvidenceConflict(ValueError):
    """The same input snapshot already has different recorded evidence."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode('utf-8')


def fingerprint(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def _validate_result(result, settings):
    """Check the detector contract using its existing feature decision helper."""
    canonical(result)
    if not isinstance(result, dict) or type(result.get('alert')) is not bool:
        raise ValueError('Malformed detector result')
    if set(result.get('features', {})) != set(ks.FEATURES):
        raise ValueError('Detector result feature mismatch')
    for values in result['features'].values():
        for key in ('ks_statistic', 'p_value'):
            value = values[key]
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError('Invalid detector statistic or p-value')
        if type(values.get('drift_detected')) is not bool:
            raise ValueError('Invalid detector feature decision')
    flagged = []
    for feature in settings['features']:
        values = result['features'][feature]
        expected = ks.feature_alert(values['ks_statistic'], values['p_value'],
            alpha=settings['alpha']/len(settings['features']), min_ks=settings['min_ks'])
        if values['drift_detected'] != expected:
            raise ValueError('Contradictory per-feature detector decision')
        if expected:
            flagged.append(feature)
    if (result.get('drifted_features') != flagged
            or type(result.get('drifted_feature_count')) is not int
            or result['drifted_feature_count'] != len(flagged)):
        raise ValueError('Contradictory detector feature list/count')
    if result['alert'] != (len(flagged) >= settings['minimum_features']):
        raise ValueError('Contradictory final detector alert')


def evaluate_plan(reference, plan, *, replay_id):
    """Only evaluated windows receive boolean alerts; every other alert is null.

    success requires at least one window and every window actually evaluated.
    It does not represent statistical-performance acceptance or model health.
    """
    settings = deepcopy(DETECTOR_SETTINGS)
    report = {'schema_version': 1, 'replay_id': replay_id, 'status': 'monitoring_failure',
              'success': False, 'evaluation_complete': False,
              'detector': {'api': 'src.drift.ks.detect_window', 'settings': settings,
                           'method': 'asymp', 'corrected_alpha': settings['alpha']/len(settings['features']),
                           'minimum_reference_samples': 100},
              'reference': {}, 'windows': [], 'errors': []}
    try:
        if not isinstance(replay_id, str) or not replay_id.strip():
            raise ValueError('Expected replay identity required')
        if not isinstance(reference, DriftReference) or not isinstance(plan, WindowPlan):
            raise ValueError('Expected DriftReference and WindowPlan')
        values = reference.values.copy(deep=True)
        provenance = deepcopy(reference.provenance)
        if not re.fullmatch('[0-9a-f]{64}', provenance.get('sha256', '')):
            raise ValueError('Invalid reference fingerprint')
        if list(values.columns) != list(ks.FEATURES) or not 100 <= len(values) <= MAX_REFERENCE_ROWS:
            raise ValueError('Reference shape mismatch')
        if not np.isfinite(values.to_numpy(dtype=float)).all():
            raise ValueError('Nonfinite reference values')
        if provenance.get('rows') != len(values):
            raise ValueError('Reference row-count metadata mismatch')
        report['reference'] = provenance
        # Bind the exact numeric view as well as the original CSV fingerprint.
        report['reference_values_sha256'] = fingerprint(values.to_dict(orient='list'))
        report['planning_status'] = plan.status
        report['planning_error'] = plan.error
        report['execution'] = {
            'python': platform.python_version(),
            'versions': {name: importlib.metadata.version(name) for name in ('numpy','pandas','scipy')},
            'detector_source_sha256': hashlib.sha256(Path(ks.__file__).read_bytes()).hexdigest()}
        # Environment/result differences under this same input identity are conflicts,
        # not new filenames that would hide an inconsistent retry.
        report['snapshot_id'] = fingerprint({'reference': provenance,
            'reference_values_sha256': report['reference_values_sha256'],
            'detector': report['detector'], 'replay_id': replay_id,
            'plan': {'status': plan.status, 'error': plan.error, 'windows': plan.windows}})
        if plan.status == 'monitoring_failure':
            raise ValueError(plan.error or 'Window planning failed')
        if plan.status not in ('planned', 'insufficient_data'):
            raise ValueError('Unknown window planning status')
        total = 0
        for window in plan.windows:
            count = window['records']
            if type(count) is not int or not 1 <= count <= WINDOW_SIZE:
                raise ValueError('Malformed planned window size')
            total += count
            if total > 10000:
                raise ValueError('Window plan exceeds existing bounded safety limit')
            if window['replay_id'] != replay_id or len(window['membership']) != count:
                raise ValueError('Planned replay or membership mismatch')
            if len(window['feature_rows']) != count:
                raise ValueError('Planned feature-row count mismatch')
            status = window['status']
            if status not in ('ready', 'insufficient_data', 'invalid_monitored_features', 'monitoring_failure'):
                raise ValueError('Unknown planned window status')
            entry = deepcopy({key: value for key,value in window.items() if key != 'feature_rows'})
            entry.update(alert=None, ks_results=None,
                         monitored_values_sha256=fingerprint(window['feature_rows']))
            report['windows'].append(entry)
            if status == 'monitoring_failure':
                report['errors'].append(f'Window {window["window_index"]}: planning failure')
                continue
            if status != 'ready':
                continue
            if window['complete'] is not True or count != WINDOW_SIZE or window['issues']:
                entry.update(status='monitoring_failure', error='Ready window is not complete and valid')
                report['errors'].append(entry['error'])
                continue
            try:
                current = pd.DataFrame(window['feature_rows'])
                result = ks.detect_window(values, current, **deepcopy(settings))
                _validate_result(result, settings)
                entry.update(status='evaluated', ks_results=result, alert=result['alert'])
            except Exception as exc:
                entry.update(status='monitoring_failure', error=f'{type(exc).__name__}: {exc}')
                report['errors'].append(f'Window {window["window_index"]}: {entry["error"]}')
        statuses = {w['status'] for w in report['windows']}
        if report['errors'] or 'monitoring_failure' in statuses:
            report['status'] = 'monitoring_failure'
        elif 'invalid_monitored_features' in statuses:
            report['status'] = 'invalid_monitored_features'
        elif not statuses or 'insufficient_data' in statuses:
            report['status'] = 'insufficient_data'
        else:
            report['status'] = 'evaluated'
        report['evaluation_complete'] = bool(statuses) and statuses == {'evaluated'} and not report['errors']
        report['success'] = report['evaluation_complete']
    except Exception as exc:
        report.update(status='monitoring_failure', success=False, evaluation_complete=False)
        report['errors'].append(f'{type(exc).__name__}: {exc}')
    report['evaluated_windows'] = sum(w['status'] == 'evaluated' for w in report['windows'])
    return report


def persist_report(run_dir, report):
    """Atomic no-clobber publication on the local Linux/WSL filesystem.

    Identical retry bytes reuse one file. Conflicts preserve the previous file.
    No append, overwrite, timestamps or UUID filenames are used.
    """
    root = Path(run_dir)
    if not root.is_dir():
        raise ValueError('Run directory must already exist')
    snapshot_id = report.get('snapshot_id', '')
    if not re.fullmatch('[0-9a-f]{64}', snapshot_id):
        raise ValueError('No valid input snapshot identity to persist')
    data = canonical(report) + b'\n'
    directory = root/'drift'/'evaluations'
    directory.mkdir(parents=True, exist_ok=True)
    target = directory/f'{snapshot_id}.json'
    descriptor, temp_name = tempfile.mkstemp(prefix='.evidence-',suffix='.tmp',dir=directory)
    temporary = Path(temp_name)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target) # atomic exclusive publication; never replaces
            state = 'persisted'
            directory_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except FileExistsError:
            if target.read_bytes() != data:
                raise EvidenceConflict('Inconsistent evidence for the same input snapshot')
            state = 'reused'
    finally:
        temporary.unlink(missing_ok=True)
    return {'status': state, 'path': str(target)}


def evaluate_and_persist(reference, plan, *, replay_id, run_dir):
    report = evaluate_plan(reference, plan, replay_id=replay_id)
    try:
        persistence = persist_report(run_dir, report)
    except Exception as exc:
        report = deepcopy(report)
        report.update(status='monitoring_failure', success=False,
                      failure_stage='persistence')
        report['errors'].append(f'{type(exc).__name__}: {exc}')
        persistence = {'status': 'monitoring_failure', 'path': None}
    return {**report, 'persistence': persistence}
