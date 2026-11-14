"""Deterministic record-count windows over a bounded committed-record snapshot.

This increment prepares windows only; it does not execute KS or alter Spark.
"""
from dataclasses import dataclass
import math
from src.drift.ks import FEATURES
from src.producer.events import validate_event

WINDOW_SIZE = 1000
DETECTOR_SETTINGS = {'features': list(FEATURES), 'minimum_features': 2,
                     'alpha': .05, 'min_ks': .10, 'window_size': WINDOW_SIZE}


@dataclass
class WindowPlan:
    windows: list
    status: str
    error: str | None = None

    @property
    def planning_complete(self):
        # Planning correctness only, not KS evaluation or statistical acceptance.
        return self.status != 'monitoring_failure'


def plan_windows(records, *, replay_id, topic, max_records=1000):
    """Rebuild from committed batches; never append state on callback retries.

    Ordering is defined only for exactly one observed topic/partition. The later
    Spark integration must also preflight broker topic partition count == 1.
    Kafka offset gaps are allowed; source rows must be continuous from zero.
    Capture changes close partial windows instead of filling them across captures.
    """
    windows = []
    try:
        if not isinstance(replay_id, str) or not replay_id.strip():
            raise ValueError('Expected replay identity required')
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError('Expected topic required')
        if type(max_records) is not int or not 1 <= max_records <= 10000:
            raise ValueError('Input safety limit must be within 1..10000')
        ordered = []
        partitions, offsets = set(), set()
        for record in records:
            if len(ordered) >= max_records:
                raise ValueError('Input safety cap exceeded')
            if not isinstance(record, dict) or record.get('valid') is not True:
                raise ValueError('Malformed or invalid transport record')
            coordinates = record.get('kafka')
            if not isinstance(coordinates, dict) or coordinates.get('topic') != topic:
                raise ValueError('Unexpected topic or malformed Kafka metadata')
            partition, offset = coordinates.get('partition'), coordinates.get('offset')
            if type(partition) is not int or partition < 0 or type(offset) is not int or offset < 0:
                raise ValueError('Invalid Kafka coordinates')
            partitions.add(partition)
            if len(partitions) != 1:
                raise ValueError('Multiple partitions have no supported global order')
            if offset in offsets:
                raise ValueError('Duplicate Kafka offset')
            offsets.add(offset)
            event = validate_event(record.get('event'))
            if event['replay_id'] != replay_id:
                raise ValueError('Unexpected replay identity')
            if event['source_capture'] is None:
                raise ValueError('Missing source capture identity')
            ordered.append((offset, record))
        ordered.sort(key=lambda item: item[0])
        # Validate the entire snapshot before reporting any ready windows.
        source_file = None
        for index, (_, record) in enumerate(ordered):
            event = record['event']
            if event['source_row'] != index:
                raise ValueError('Source-row continuity violation (expected rows from zero)')
            if source_file is None:
                source_file = event['source_file']
            if event['source_file'] != source_file:
                raise ValueError('Source file changed within replay')

        pending = []
        capture = None

        def finish(reason):
            nonlocal pending
            if not pending:
                return
            issues, feature_rows, membership = [], [], []
            for offset, record in pending:
                event = record['event']
                feature_row = {}
                invalid = []
                for feature in FEATURES:
                    value = event['features'].get(feature)
                    try:
                        finite = type(value) in (int, float) and math.isfinite(value)
                    except OverflowError:
                        finite = False
                    if not finite:
                        invalid.append(feature)
                    feature_row[feature] = value
                membership.append({'event_id': event['event_id'], 'source_row': event['source_row'],
                                   'topic': topic, 'partition': next(iter(partitions)), 'offset': offset})
                if invalid:
                    issues.append({'event_id': event['event_id'], 'features': invalid})
                feature_rows.append(feature_row)
            complete = len(pending) == WINDOW_SIZE
            status = ('invalid_monitored_features' if issues else
                      'ready' if complete else 'insufficient_data')
            windows.append({'window_index': len(windows), 'replay_id': replay_id,
                            'source_capture': capture, 'source_file': source_file,
                            'status': status, 'complete': complete,
                            'records': len(pending), 'close_reason': reason,
                            'membership': membership, 'feature_rows': feature_rows,
                            'issues': issues, 'alert': None})
            pending = []

        for offset, record in ordered:
            current_capture = record['event']['source_capture']
            if capture is not None and current_capture != capture:
                finish('capture_boundary')
            capture = current_capture
            pending.append((offset, record))
            if len(pending) == WINDOW_SIZE:
                finish('window_full')
        finish('snapshot_end')
        return WindowPlan(windows, 'planned' if ordered else 'insufficient_data')
    except Exception as exc:
        return WindowPlan([], 'monitoring_failure', str(exc))
