"""Versioned replay transport schema; no inference or feature selection."""
import csv
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re


def validate_event(event):
    if not isinstance(event, dict) or type(event.get('schema_version')) is not int or event['schema_version'] != 1:
        raise ValueError('Unsupported event schema')
    for name in ('event_id', 'replay_id', 'source_file', 'replayed_at'):
        if not isinstance(event.get(name), str) or not event[name].strip():
            raise ValueError(f'Missing/invalid {name}')
    try:
        stamp = datetime.fromisoformat(event['replayed_at'])
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise ValueError('Timestamp must have timezone')
    except (ValueError, TypeError) as exc:
        raise ValueError('Invalid replay timestamp') from exc
    if type(event.get('source_row')) is not int or event['source_row'] < 0:
        raise ValueError('Invalid source_row')
    if event['event_id'] != f"{event['replay_id']}:{event['source_row']}":
        raise ValueError('Event identity mismatch')
    for name in ('label', 'source_capture'):
        if name not in event or (event[name] is not None and
                                 (not isinstance(event[name], str) or not event[name].strip())):
            raise ValueError(f'Invalid {name}')
    features = event.get('features')
    if not isinstance(features, dict) or not features:
        raise ValueError('Features must be a nonempty object')
    for name, value in features.items():
        if not isinstance(name, str) or not name or name in {'Label', 'SourceFile', 'Target'}:
            raise ValueError('Invalid feature name')
        if value is not None and (type(value) not in (int, float) or
                                  (type(value) is float and not math.isfinite(value))):
            raise ValueError(f'Invalid numeric feature: {name}')
    invalid = event.get('invalid_features')
    if not isinstance(invalid, list) or any(not isinstance(f, str) for f in invalid):
        raise ValueError('Invalid feature-quality metadata')
    if len(invalid) != len(set(invalid)) or set(invalid) != {f for f,v in features.items() if v is None}:
        raise ValueError('Feature-quality metadata mismatch')
    return event


def numeric(value):
    value = value.strip()
    if not value:
        return None
    try:
        if re.fullmatch(r'[+-]?\d+', value):
            return int(value)
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, OverflowError):
        return None


def row_to_event(row, *, source_file, source_row, replay_id):
    if 'Target' in row:
        raise ValueError('Use source/capture-aware CSV, not ML train/test output')
    features = {k: numeric(v) for k,v in row.items() if k not in {'Label', 'SourceFile'}}
    event = {'schema_version': 1, 'event_id': f'{replay_id}:{source_row}',
             'replay_id': replay_id, 'source_file': source_file, 'source_row': source_row,
             'source_capture': row.get('SourceFile', '').strip() or None,
             'label': row.get('Label', '').strip() or None,
             'replayed_at': datetime.now(timezone.utc).isoformat(),
             'features': features, 'invalid_features': [k for k,v in features.items() if v is None]}
    return validate_event(event)


def csv_events(path, replay_id):
    """One parsed record at a time; row identities include no header."""
    path = Path(path)
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.reader(stream, strict=True)
        try:
            headers = [h.strip() for h in next(reader)]
        except StopIteration as exc:
            raise ValueError('Empty CSV') from exc
        if not headers or any(not h for h in headers) or len(headers) != len(set(headers)):
            raise ValueError('Empty/duplicate CSV headers; use prepared capture-aware CSV')
        if 'Label' not in headers or set(headers) <= {'Label', 'SourceFile'}:
            raise ValueError('Expected source features and Label')
        for index, values in enumerate(reader):
            if len(values) != len(headers):
                raise ValueError(f'CSV width mismatch at source row {index}')
            yield row_to_event(dict(zip(headers, values)), source_file=path.name,
                               source_row=index, replay_id=replay_id)


def encode_event(event):
    validate_event(event)
    return json.dumps(event, allow_nan=False, separators=(',', ':')).encode('utf-8')


def decode_event(payload):
    def reject_constant(value):
        raise ValueError(f'Invalid JSON constant {value}')
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f'Duplicate JSON key {key}')
            result[key] = value
        return result
    try:
        return validate_event(json.loads(payload, parse_constant=reject_constant,
                                         object_pairs_hook=unique_pairs))
    except (TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError('Invalid JSON event') from exc
