"""Repeat bounded CSV replay using the existing Kafka/Spark evidence pipeline.

Running this command contacts Kafka and starts Spark. It does not train models,
capture network packets, start services, or delete topics/evidence.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import signal
import sys
import uuid

from scripts.preflight_replay import preflight
from src.producer.events import csv_events
from src.producer.kafka_config import positive, producer_config
from src.producer.replay import publish
from src.streaming.drift_monitor import (
    bound_reference, drift_succeeded, failed_summary, prepare_drift_config, require_single_partition,
)
from src.streaming.inference import FEATURES_PATH, MODEL_PATH, StreamingInference
from src.streaming.spark_consumer import supervise, write_json
from src.streaming.validation import source_options
from src.streaming.verify_inference import verify_inference
from src.streaming.verify_output import verify

MAX_INPUT_BYTES = 64 * 1024 * 1024


def fingerprint(path):
    digest = hashlib.sha256()
    size = 0
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            size += len(chunk)
            if size > MAX_INPUT_BYTES:
                raise ValueError('Input exceeds 64 MiB safety limit')
            digest.update(chunk)
    return digest.hexdigest()


def create_topic(brokers, topic, timeout):
    """Creation must succeed; an existing topic is never accepted or deleted."""
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({'bootstrap.servers': brokers})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)],
        request_timeout=timeout,
    )
    futures[topic].result(timeout=timeout)
    require_single_partition(brokers, topic)
    consumer = Consumer({'bootstrap.servers': brokers,
                         'group.id': f'ids-freshness-{uuid.uuid4()}',
                         'enable.auto.commit': False,
                         'allow.auto.create.topics': False})
    try:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, 0), timeout=timeout, cached=False)
        if (low, high) != (0, 0):
            raise ValueError('New topic is not empty; refusing to publish')
    finally:
        consumer.close()


def run_analysis(source, *, evidence_root='data/processed/spark', max_rows=1000,
                 brokers='127.0.0.1:9092', rate=100., timeout=180.,
                 delivery_timeout=35., batch_size=100, drift_reference=None):
    run_id = str(uuid.uuid4())
    root = Path(evidence_root).resolve() / run_id
    root.mkdir(parents=True, exist_ok=False)
    report = {'success': False, 'accounting_complete': False, 'run_id': run_id,
              'created_at': datetime.now(timezone.utc).isoformat(), 'status': 'incomplete',
              'run_dir': str(root), 'stage': 'preflight', 'errors': []}
    spark_report = None
    config = {}

    def persist():
        # Spark never writes this public summary for an orchestrated run.
        summary = dict(spark_report or {})
        if drift_reference is not None and 'drift' not in summary:
            summary['drift'] = failed_summary(config, '; '.join(report['errors']) or
                                             'Orchestration has not completed')
        summary.update(success=report['success'], accounting_complete=report['accounting_complete'],
                       created_at=report['created_at'],
                       analysis={key: value for key, value in report.items() if key != 'spark'})
        summary['errors'] = list(summary.get('errors', [])) + report['errors']
        if report['status'] == 'incomplete':
            summary['errors'].append('Orchestration has not completed')
        # Public success is the last atomic write, after all gates and metadata.
        write_json(root / 'analysis.json', report)
        write_json(root / 'summary.json', summary)

    def stage(name):
        report['stage'] = name
        persist()

    def terminated(signum, frame):
        raise KeyboardInterrupt(f'Terminated by signal {signal.Signals(signum).name}')

    persist()
    previous_sigterm = signal.signal(signal.SIGTERM, terminated)
    try:
        if type(max_rows) is not int or not 1 <= max_rows <= 10000:
            raise ValueError('max_rows must be between 1 and 10000 (Spark cap)')
        positive(timeout, 'timeout')
        positive(delivery_timeout, 'delivery_timeout')
        # publish() validates rate too; reject it before creating any topic.
        if not math.isfinite(rate) or rate < 0:
            raise ValueError('rate must be finite and nonnegative')
        topic = f'ids.analysis-{run_id}'
        source_options(brokers, topic, batch_size)
        config = dict(brokers=brokers, topic=topic, replay_id=str(uuid.uuid4()),
                      run_dir=str(root), max_records=max_rows, batch_size=batch_size,
                      enable_inference=True, enable_drift=drift_reference is not None,
                      drift_reference=drift_reference, orchestrated=True)
        source = Path(source).resolve()
        original_hash = fingerprint(source)
        snapshot_dir = root / 'input'
        snapshot_dir.mkdir()
        snapshot = snapshot_dir / source.name
        # Exclusive snapshot, bounded independently of the source's stat size.
        with source.open('rb') as incoming, snapshot.open('xb') as outgoing:
            size = 0
            for chunk in iter(lambda: incoming.read(1024 * 1024), b''):
                size += len(chunk)
                if size > MAX_INPUT_BYTES:
                    raise ValueError('Input exceeds 64 MiB safety limit')
                outgoing.write(chunk)
        if fingerprint(snapshot) != original_hash or fingerprint(source) != original_hash:
            raise ValueError('Input changed while snapshotting')
        features_hash = fingerprint(FEATURES_PATH)
        checked = preflight(snapshot, FEATURES_PATH, max_rows=max_rows)
        model_hash = fingerprint(MODEL_PATH)
        engine = StreamingInference()  # verifies actual model/manifest order and classes
        if fingerprint(MODEL_PATH) != model_hash or fingerprint(FEATURES_PATH) != features_hash:
            raise ValueError('Model artifacts changed during verification')
        report['model'] = {'path': str(MODEL_PATH), 'sha256': model_hash,
                           'features_sha256': features_hash,
                           'required_features': len(engine.features)}
        report['input'] = {'source': str(source), 'snapshot': str(snapshot),
                           'sha256': original_hash, 'preflight': checked}
        report['counts'] = {'source_rows': checked['rows'],
                            'numeric_valid_rows': checked['valid_rows'],
                            'numeric_invalid_rows': checked['invalid_rows'],
                            'transport_valid': None, 'transport_invalid': None,
                            'predictions': None}
        config['expected_count'] = checked['rows']
        config = prepare_drift_config(config)
        if drift_reference is not None:
            # Reuse the existing planner's capture-provenance contract.
            if any(event['source_capture'] is None for event in csv_events(snapshot, 'preflight')):
                raise ValueError('Drift requires nonempty SourceFile for every row')
        report.update(topic=topic, replay_id=config['replay_id'])
        stage('topic_creation')
        create_topic(brokers, topic, delivery_timeout)
        stage('input_identity')
        if fingerprint(source) != original_hash or fingerprint(snapshot) != original_hash:
            raise ValueError('Input changed since preflight; refusing to publish')
        if (fingerprint(MODEL_PATH) != report['model']['sha256'] or
                fingerprint(FEATURES_PATH) != report['model']['features_sha256']):
            raise ValueError('Model artifacts changed since preflight')
        if config['enable_drift']:
            bound_reference(config)
        stage('publish')
        from confluent_kafka import Producer
        delivery = publish(Producer(producer_config(brokers, topic)),
                           csv_events(snapshot, config['replay_id']), topic,
                           max_events=checked['rows'], rate=rate, timeout=delivery_timeout)
        delivery['replay_id'] = config['replay_id']
        report['producer'] = delivery
        write_json(root / 'producer.json', delivery)
        if (delivery.get('success') is not True or delivery.get('interrupted') is not False
                or delivery.get('errors') != []
                or any(delivery.get(key) != checked['rows'] for key in
                       ('attempted', 'enqueued', 'delivered'))
                or any(delivery.get(key) != 0 for key in
                       ('delivery_failed', 'unconfirmed'))):
            raise ValueError('Producer acknowledgements do not match bounded input')

        stage('spark')
        if (fingerprint(MODEL_PATH) != model_hash or
                fingerprint(FEATURES_PATH) != features_hash):
            raise ValueError('Model artifacts changed before Spark inference')
        spark_report = supervise(config, timeout, reserved=True)
        report['spark'] = spark_report
        report['counts'].update(transport_valid=spark_report.get('valid'),
                                transport_invalid=spark_report.get('invalid'),
                                predictions=spark_report.get('inference', {}).get('predictions'))
        report['accounting_complete'] = spark_report.get('accounting_complete') is True
        if spark_report.get('success') is not True or not report['accounting_complete']:
            raise ValueError('Spark acceptance failed or accounting is incomplete')
        if (fingerprint(MODEL_PATH) != model_hash or
                fingerprint(FEATURES_PATH) != features_hash):
            raise ValueError('Model artifacts changed during Spark inference')
        stage('source_verification')
        if fingerprint(snapshot) != original_hash:
            raise ValueError('Published snapshot changed')
        report['source_verification'] = verify(root, snapshot, summary=spark_report)
        if report['source_verification'].get('success') is not True:
            raise ValueError('Source verification failed')
        stage('inference_verification')
        inference = verify_inference(root)
        report['inference'] = inference
        report['counts']['predictions'] = inference.get('predictions')
        if inference.get('verified') is not True or inference.get('predictions') != checked['rows']:
            raise ValueError('Inference verification failed or prediction count mismatch')
        if config['enable_drift']:
            stage('drift_verification')
            if not drift_succeeded(spark_report.get('drift', {})):
                raise ValueError('Optional drift evaluation did not succeed')
        report.update(success=True, stage='complete', status='complete')
        persist()
    except (Exception, KeyboardInterrupt, SystemExit) as exc:
        report.update(success=False, status='failed')
        report['failed_stage'] = report['stage']
        report['errors'].append(str(exc) or type(exc).__name__)
        persist()
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--evidence-root', default='data/processed/spark')
    parser.add_argument('--max-rows', type=int, default=1000)
    parser.add_argument('--brokers', default='127.0.0.1:9092')
    parser.add_argument('--rate', type=float, default=100.)
    parser.add_argument('--timeout', type=float, default=180.)
    parser.add_argument('--delivery-timeout', type=float, default=35.)
    parser.add_argument('--batch-size', type=int, default=100)
    parser.add_argument('--drift-reference')
    args = parser.parse_args()
    try:
        report = run_analysis(**vars(args))
    except Exception as exc:
        report = {'success': False, 'accounting_complete': False, 'errors': [str(exc)]}
    print(json.dumps(report, indent=2, allow_nan=False))
    return 0 if report['success'] else 1


if __name__ == '__main__':
    sys.exit(main())
