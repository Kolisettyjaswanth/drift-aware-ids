"""Opt-in bounded drift orchestration; transport remains the commit authority."""
from src.streaming.drift_reference import load_reference
from src.streaming.drift_windows import plan_windows
from src.streaming.drift_evaluation import evaluate_and_persist
from src.streaming.validation import read_records


class DriftMonitoringError(RuntimeError):
    pass


def prepare_drift_config(config):
    result = dict(config)
    if result.get('enable_drift', False):
        if not result.get('drift_reference'):
            raise ValueError('--drift-reference is required with --enable-drift')
        reference = load_reference(result['drift_reference'])
        result['drift_reference'] = reference.provenance['path']
        result['drift_reference_sha256'] = reference.provenance['sha256']
    return result


def bound_reference(config):
    expected = config.get('drift_reference_sha256')
    if not expected:
        raise ValueError('Missing preflight reference SHA-256')
    return load_reference(config['drift_reference'], expected_sha256=expected)


def require_single_partition(brokers, topic):
    from confluent_kafka.admin import AdminClient
    client = AdminClient({'bootstrap.servers': brokers, 'socket.timeout.ms': 10000})
    metadata = client.list_topics(topic=topic, timeout=10)
    info = metadata.topics.get(topic)
    if info is None or info.error is not None:
        raise DriftMonitoringError(f'Kafka metadata unavailable for {topic}')
    if len(info.partitions) != 1:
        raise DriftMonitoringError('Drift requires exactly one configured Kafka partition')
    if any(partition.error is not None for partition in info.partitions.values()):
        raise DriftMonitoringError('Kafka partition metadata contains errors')


def failed_summary(config, reason):
    return {'status': 'monitoring_failure', 'success': False, 'evaluation_complete': False,
            'reference_sha256': config.get('drift_reference_sha256'),
            'evaluated_windows': 0, 'evidence_paths': [], 'errors': [str(reason)]}


def drift_succeeded(summary):
    return (summary.get('success') is True and
            summary.get('evaluation_complete') is True and summary.get('status') == 'evaluated')


class DriftMonitor:
    def __init__(self, config, reference):
        self.config = config
        self.reference = reference
        self.last_report = None
        self.evidence_paths = set()
        self.failure = None

    def update(self):
        # Failure is sticky for this run, even if a later call would succeed.
        if self.failure is not None:
            raise DriftMonitoringError(self.failure)
        try:
            require_single_partition(self.config['brokers'], self.config['topic'])
            plan = plan_windows(read_records(self.config['run_dir']),
                                replay_id=self.config['replay_id'], topic=self.config['topic'],
                                max_records=self.config['max_records'])
            report = evaluate_and_persist(self.reference, plan,
                replay_id=self.config['replay_id'], run_dir=self.config['run_dir'])
            self.last_report = report
            if report['persistence']['path']:
                self.evidence_paths.add(report['persistence']['path'])
            # Partial snapshots are expected between micro-batches. Finalization
            # requires actual complete evaluation, never planning_complete alone.
            if report['status'] not in ('evaluated', 'insufficient_data') or (
                    report['status'] == 'evaluated' and not report['success']):
                raise DriftMonitoringError('; '.join(report['errors']) or report['status'])
            return self.summary()
        except KeyboardInterrupt:
            self.failure = 'Drift monitoring interrupted'
            raise
        except Exception as exc:
            self.failure = str(exc) or type(exc).__name__
            raise DriftMonitoringError(self.failure) from exc

    def finalize(self):
        self.update() # also rechecks configured partition count at query completion
        result = self.summary()
        if not drift_succeeded(result):
            raise DriftMonitoringError('Drift monitoring incomplete: '+result['status'])
        return result

    def summary(self):
        report = self.last_report
        if report is None:
            return failed_summary(self.config, self.failure or 'No drift evaluation completed')
        result = {'status': report['status'], 'success': report['success'],
                  'evaluation_complete': report['evaluation_complete'],
                  'reference_sha256': self.reference.provenance['sha256'],
                  'snapshot_id': report.get('snapshot_id'),
                  'evaluated_windows': report['evaluated_windows'],
                  'evidence_paths': sorted(self.evidence_paths),
                  'errors': list(report['errors']),
                  'windows': [{'window_index': w['window_index'], 'status': w['status'],
                               'records': w['records'], 'alert': w['alert'],
                               'source_capture': w['source_capture']} for w in report['windows']]}
        if self.failure is not None:
            result['success'] = False
            result['evaluation_complete'] = False
            if result['status'] in ('evaluated', 'insufficient_data'):
                result['status'] = 'monitoring_failure'
            if self.failure not in result['errors']:
                result['errors'].append(self.failure)
        return result
