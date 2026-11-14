"""Strict, bounded loading of existing BENIGN KS reference CSVs.

No sampling, imputation or detector implementation is introduced here.
"""
import csv
from dataclasses import dataclass
import hashlib
import io
from pathlib import Path

import numpy as np
import pandas as pd
from src.drift.ks import FEATURES

MAX_REFERENCE_BYTES = 64 * 1024 * 1024
MAX_REFERENCE_ROWS = 100000


@dataclass(frozen=True)
class DriftReference:
    values: pd.DataFrame
    provenance: dict


def _checked_rows(reader):
    try:
        yield from reader
    except csv.Error as exc:
        raise ValueError('Malformed reference CSV') from exc


def load_reference(path, *, expected_sha256=None):
    """Hash and parse the same bounded byte snapshot, before Spark is started.

    expected_sha256 can bind a later worker load to a preflight fingerprint.
    Reference row identity/independence cannot be proved by this CSV alone.
    """
    path = Path(path)
    with path.open('rb') as stream:
        raw = stream.read(MAX_REFERENCE_BYTES + 1)
    if len(raw) > MAX_REFERENCE_BYTES:
        raise ValueError('Reference exceeds byte safety limit')
    fingerprint = hashlib.sha256(raw).hexdigest()
    if expected_sha256 is not None and fingerprint != expected_sha256:
        raise ValueError('Reference fingerprint mismatch')
    try:
        text = raw.decode('utf-8-sig')
    except UnicodeError as exc:
        raise ValueError('Reference must be UTF-8 CSV') from exc
    reader = _checked_rows(csv.reader(io.StringIO(text, newline=''), strict=True))
    try:
        headers = [name.strip() for name in next(reader)]
    except StopIteration as exc:
        raise ValueError('Empty reference CSV') from exc
    if any(not name for name in headers) or len(headers) != len(set(headers)):
        raise ValueError('Empty or duplicate reference headers')
    required = set(FEATURES) | {'Label', 'SourceFile'}
    if required - set(headers):
        raise ValueError(f'Missing reference columns: {sorted(required-set(headers))}')
    rows, captures = [], {}
    for values in reader:
        if len(values) != len(headers):
            raise ValueError('Reference CSV width mismatch')
        if len(rows) >= MAX_REFERENCE_ROWS:
            raise ValueError('Reference exceeds row safety limit')
        record = dict(zip(headers, values))
        capture = record['SourceFile'].strip()
        if record['Label'].strip() != 'BENIGN' or not capture:
            raise ValueError('Reference requires BENIGN labels and nonempty SourceFile')
        try:
            row = [float(record[feature]) for feature in FEATURES]
        except (ValueError, OverflowError) as exc:
            raise ValueError('Reference contains invalid monitored features') from exc
        if not np.isfinite(row).all():
            raise ValueError('Reference contains nonfinite monitored features')
        rows.append(row)
        captures[capture] = captures.get(capture, 0) + 1
    if len(rows) < 100:
        raise ValueError('Reference needs at least 100 finite rows per feature')
    return DriftReference(
        pd.DataFrame(rows, columns=FEATURES),
        {'path': str(path.resolve()), 'sha256': fingerprint, 'bytes': len(raw),
         'rows': len(rows), 'features': list(FEATURES),
         'capture_counts': dict(sorted(captures.items())),
         'excluded_rows': 0, 'preprocessing': 'strict finite values; no imputation or dropping'})
