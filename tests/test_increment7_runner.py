"""Bounded orchestration tests: no broker, JVM, or retraining."""
import csv
import json
import multiprocessing
import os
import signal
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from scripts import run_analysis as runner
from src.producer.events import csv_events
from src.streaming.spark_consumer import supervise, summary_path, worker, write_json
from src.streaming.validation import SPARK_VERSION
from src.streaming.validation import summarize


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / 'flows.csv'
        with self.source.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerows([['a', 'Label', 'SourceFile'],
                              ['1', 'BENIGN', 'capture'], ['', '', 'capture']])
        self.features = self.root / 'features.json'
        self.features.write_text('["a"]')
        self.model = self.root / 'model.joblib'
        self.model.write_bytes(b'unchanged-model')
        self.events = []
        self.topic = self.mock('create_topic')
        self.publisher = self.mock('publish', side_effect=self.publish)
        self.spark = self.mock('supervise', side_effect=self.spark_run)
        self.mock('FEATURES_PATH', self.features)
        self.mock('MODEL_PATH', self.model)
        self.mock('StreamingInference', return_value=SimpleNamespace(features=['a']))
        producer = patch('confluent_kafka.Producer')
        producer.start()
        self.addCleanup(producer.stop)

    def mock(self, name, *args, **kwargs):
        mocked = patch.object(runner, name, *args, **kwargs)
        value = mocked.start()
        self.addCleanup(mocked.stop)
        return value

    def publish(self, client, events, topic, **kwargs):
        self.events = list(events)
        count = len(self.events)
        return dict(success=True, attempted=count, enqueued=count, delivered=count,
                    delivery_failed=0, unconfirmed=0, errors=[], interrupted=False)

    def spark_run(self, config, timeout, *, reserved):
        self.assertTrue(reserved)
        root = Path(config['run_dir'])
        self.assertTrue(root.is_dir())
        self.assertTrue(config['orchestrated'])
        self.assertFalse(json.loads((root / 'summary.json').read_text())['success'])
        write_json(root / 'config.json', config)
        (root / 'batches').mkdir()
        (root / 'predictions').mkdir()
        records, predictions = [], []
        for index, event in enumerate(self.events):
            kafka = dict(topic=config['topic'], partition=0, offset=index)
            records.append(dict(valid=True, event=event, kafka=kafka))
            predictions.append(dict(event_id=event['event_id'], replay_id=event['replay_id'],
                                    source_row=event['source_row'], kafka=kafka,
                                    prediction=0, prediction_label='BENIGN',
                                    probability_benign=.8, probability_attack=.2))
        for directory, rows in [('batches', records), ('predictions', predictions)]:
            (root / directory / 'batch-000.jsonl').write_text(
                ''.join(json.dumps(row) + '\n' for row in rows))
        report = summarize(records, config['replay_id'], config['expected_count'],
                           config['topic'], config['max_records'])
        report.update(accounting_complete=True, errors=[],
                      inference=dict(verified=True, predictions=len(predictions)))
        write_json(summary_path(config), report)
        self.assertFalse(json.loads((root / 'summary.json').read_text())['success'])
        return report

    def run_analysis(self, **kwargs):
        return runner.run_analysis(self.source, evidence_root=self.root / 'runs', **kwargs)

    def assert_failed(self, report, stage):
        self.assertFalse(report['success'])
        self.assertEqual(report['failed_stage'], stage)
        summary = json.loads((Path(report['run_dir']) / 'summary.json').read_text())
        self.assertFalse(summary['success'])
        self.assertEqual(summary['accounting_complete'], report['accounting_complete'])
        self.assertEqual(summary['analysis']['failed_stage'], stage)
        self.assertTrue(summary['errors'])

    def test_success_preserves_invalid_provenance_and_verifies_actual_files(self):
        report = self.run_analysis()
        self.assertTrue(report['success'], report)
        self.assertEqual(report['counts'], dict(source_rows=2, numeric_valid_rows=1,
            numeric_invalid_rows=1, transport_valid=2, transport_invalid=0, predictions=2))
        self.assertEqual(self.events[1]['source_row'], 1)
        self.assertEqual(self.events[1]['invalid_features'], ['a'])
        self.assertIsNone(self.events[1]['label'])
        self.assertEqual(report['input']['sha256'], runner.fingerprint(self.source))
        from backend.app.run_evidence import read_run_summary
        summary = read_run_summary(self.root / 'runs', report['run_id'])
        self.assertTrue(summary['success'])

    def test_fresh_runs_and_replays(self):
        first, second = self.run_analysis(), self.run_analysis()
        for key in ('run_id', 'run_dir', 'topic', 'replay_id'):
            self.assertNotEqual(first[key], second[key])

    def test_row_cap_rejected_before_topic(self):
        for limit in (0, 10001):
            with self.subTest(limit=limit):
                self.assert_failed(self.run_analysis(max_rows=limit), 'preflight')
        self.topic.assert_not_called()

    def test_oversized_csv_rejected_before_topic(self):
        self.assert_failed(self.run_analysis(max_rows=1), 'preflight')
        self.topic.assert_not_called()

    def test_model_mismatch_before_topic(self):
        runner.StreamingInference.side_effect = ValueError('manifest mismatch')
        self.assert_failed(self.run_analysis(), 'preflight')
        self.topic.assert_not_called()

    def test_source_change_before_publish(self):
        self.topic.side_effect = lambda *args: self.source.write_text('changed')
        self.assert_failed(self.run_analysis(), 'input_identity')
        self.publisher.assert_not_called()

    def test_snapshot_change_before_publish(self):
        def change(*args):
            snapshot = next((self.root / 'runs').glob('*/input/flows.csv'))
            snapshot.write_text('changed')
        self.topic.side_effect = change
        self.assert_failed(self.run_analysis(), 'input_identity')
        self.publisher.assert_not_called()

    def test_byte_cap_before_topic(self):
        self.mock('MAX_INPUT_BYTES', 4)
        self.assert_failed(self.run_analysis(), 'preflight')
        self.topic.assert_not_called()

    def test_invalid_configuration_before_topic(self):
        for options in ({'timeout': 0}, {'delivery_timeout': -1}, {'batch_size': 1001},
                        {'rate': float('nan')}, {'rate': -1}, {'brokers': ''}):
            with self.subTest(options=options):
                self.assert_failed(self.run_analysis(**options), 'preflight')
        self.topic.assert_not_called()

    def test_missing_capture_for_drift_before_topic(self):
        self.mock('prepare_drift_config', side_effect=lambda config: config)
        self.source.write_text('a,Label\n1,BENIGN\n')
        report = self.run_analysis(drift_reference='reference.csv')
        self.assert_failed(report, 'preflight')
        self.topic.assert_not_called()
        from backend.app.drift_evidence import read_drift
        drift = read_drift(self.root / 'runs', report['run_id'])
        self.assertEqual(drift['status'], 'monitoring_failure')

    def test_changed_reference_rejected_before_publish(self):
        from tests.test_drift_foundations import reference_text
        reference = self.root / 'reference.csv'
        reference.write_text(reference_text())
        self.topic.side_effect = lambda *args: reference.write_text(
            reference_text().replace('1.5', '2.5'))
        report = self.run_analysis(drift_reference=str(reference))
        self.assert_failed(report, 'input_identity')
        self.publisher.assert_not_called()

    def test_supervisor_preparation_cannot_rebind_changed_reference(self):
        from tests.test_drift_foundations import reference_text
        reference = self.root / 'reference.csv'
        reference.write_text(reference_text())
        config = runner.prepare_drift_config(dict(enable_drift=True,
                                                  drift_reference=str(reference)))
        reference.write_text(reference_text().replace('1.5', '2.5'))
        with self.assertRaisesRegex(ValueError, 'fingerprint mismatch'):
            runner.prepare_drift_config(config)

    def test_model_change_before_publish(self):
        self.topic.side_effect = lambda *args: self.model.write_bytes(b'changed')
        self.assert_failed(self.run_analysis(), 'input_identity')
        self.publisher.assert_not_called()

    def test_existing_topic_failure(self):
        self.topic.side_effect = RuntimeError('TOPIC_ALREADY_EXISTS')
        report = self.run_analysis()
        self.assert_failed(report, 'topic_creation')
        self.assertFalse(report['accounting_complete'])
        self.publisher.assert_not_called()

    def test_inexact_delivery_stops_spark(self):
        for field, value in [('delivered', 1), ('attempted', 1), ('enqueued', 1),
                             ('delivery_failed', 1), ('unconfirmed', 1),
                             ('interrupted', True), ('success', False),
                             ('errors', ['failure'])]:
            with self.subTest(field=field):
                delivery = self.publish(None, csv_events(self.source, 'test'), '', max_events=2)
                delivery[field] = value
                self.publisher.side_effect = None
                self.publisher.return_value = delivery
                self.assert_failed(self.run_analysis(), 'publish')
        self.spark.assert_not_called()

    def test_spark_incomplete(self):
        self.spark.side_effect = None
        self.spark.return_value = dict(success=False, accounting_complete=False,
                                       valid=1, invalid=0, errors=['timeout'])
        report = self.run_analysis()
        self.assert_failed(report, 'spark')
        self.assertFalse(report['accounting_complete'])
        self.assertEqual(report['counts']['transport_valid'], 1)
        self.assertIsNone(report['counts']['predictions'])

    def test_source_verification_failure(self):
        self.mock('verify', side_effect=ValueError('source mismatch'))
        report = self.run_analysis()
        self.assert_failed(report, 'source_verification')
        self.assertTrue(report['accounting_complete'])

    def test_public_success_stays_false_during_all_final_gates(self):
        from backend.app.run_evidence import read_run_summary
        original_verify, original_inference = runner.verify, runner.verify_inference
        def inspect(root, stage):
            detail = read_run_summary(self.root / 'runs', root.name)
            self.assertFalse(detail['success'])
            self.assertTrue(detail['accounting_complete'])
            self.assertEqual(detail['summary']['analysis']['stage'], stage)
            self.assertTrue(json.loads((root / 'spark-summary.json').read_text())['success'])
        def verify(root, source, **kwargs):
            inspect(root, 'source_verification')
            return original_verify(root, source, **kwargs)
        def inference(root):
            inspect(root, 'inference_verification')
            return original_inference(root)
        self.mock('verify', side_effect=verify)
        self.mock('verify_inference', side_effect=inference)
        report = self.run_analysis()
        self.assertTrue(report['success'], report)
        # The original CLI verifier still works on the finalized public summary.
        self.assertTrue(original_verify(Path(report['run_dir']), report['input']['snapshot'])['success'])

    def test_post_spark_exceptions_keep_private_success_and_public_failure(self):
        previous = signal.getsignal(signal.SIGTERM)
        for failure in (RuntimeError('unexpected failure'), KeyboardInterrupt(), SystemExit(9)):
            with self.subTest(failure=type(failure).__name__):
                with patch.object(runner, 'verify', side_effect=failure):
                    report = self.run_analysis()
                self.assert_failed(report, 'source_verification')
                self.assertTrue(report['accounting_complete'])
                self.assertTrue(json.loads((Path(report['run_dir']) /
                                             'spark-summary.json').read_text())['success'])
                self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_sigterm_after_spark_persists_failure_and_restores_handler(self):
        previous = signal.getsignal(signal.SIGTERM)
        def terminate(*args, **kwargs):
            os.kill(os.getpid(), signal.SIGTERM)
        self.mock('verify', side_effect=terminate)
        report = self.run_analysis()
        self.assert_failed(report, 'source_verification')
        self.assertTrue(report['accounting_complete'])
        self.assertIn('SIGTERM', report['errors'][0])
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_uncatchable_exit_leaves_incomplete_public_summary(self):
        # An otherwise unhandled BaseException models abrupt exit: no finalizer
        # can promote the already-written private Spark success to public success.
        class AbruptExit(BaseException):
            pass
        self.mock('verify', side_effect=AbruptExit())
        with self.assertRaises(AbruptExit):
            self.run_analysis()
        root = next((self.root / 'runs').iterdir())
        summary = json.loads((root / 'summary.json').read_text())
        self.assertFalse(summary['success'])
        self.assertEqual(summary['analysis']['status'], 'incomplete')
        self.assertEqual(summary['analysis']['stage'], 'source_verification')
        self.assertTrue(json.loads((root / 'spark-summary.json').read_text())['success'])

    def test_sigkill_after_spark_cannot_leave_public_success(self):
        context = multiprocessing.get_context('fork')  # Existing Linux/WSL runtime.
        reached_verification = context.Event()
        def wait_after_spark(*args, **kwargs):
            reached_verification.set()
            signal.pause()
        self.mock('verify', side_effect=wait_after_spark)
        process = context.Process(target=self.run_analysis)
        process.start()
        try:
            self.assertTrue(reached_verification.wait(10), 'Child did not reach verification')
            process.kill()
            process.join(10)
            self.assertEqual(process.exitcode, -signal.SIGKILL)
        finally:
            if process.is_alive():
                process.kill()
                process.join(10)
            process.close()
        root = next((self.root / 'runs').iterdir())
        summary = json.loads((root / 'summary.json').read_text())
        self.assertFalse(summary['success'])
        self.assertTrue(summary['accounting_complete'])
        self.assertEqual(summary['analysis']['status'], 'incomplete')
        self.assertEqual(summary['analysis']['stage'], 'source_verification')
        self.assertTrue(json.loads((root / 'spark-summary.json').read_text())['success'])

    def test_final_metadata_write_failure_cannot_publish_success(self):
        original = runner.write_json
        def write(path, value):
            if path.name == 'analysis.json' and value.get('success') is True:
                raise OSError('final metadata write failed')
            original(path, value)
        self.mock('write_json', side_effect=write)
        report = self.run_analysis()
        self.assert_failed(report, 'complete')
        self.assertTrue(report['accounting_complete'])

    def test_prediction_verification_failure(self):
        self.mock('verify_inference', side_effect=ValueError('prediction mismatch'))
        self.assert_failed(self.run_analysis(), 'inference_verification')

    def test_wrong_prediction_count(self):
        self.mock('verify_inference', return_value=dict(verified=True, predictions=1))
        self.assert_failed(self.run_analysis(), 'inference_verification')

    def test_interrupt_persists_failure(self):
        self.publisher.side_effect = KeyboardInterrupt()
        report = self.run_analysis()
        self.assert_failed(report, 'publish')
        self.assertFalse(report['accounting_complete'])
        self.spark.assert_not_called()

    def test_existing_run_directory_not_overwritten(self):
        (self.root / 'runs' / 'fixed').mkdir(parents=True)
        marker = self.root / 'runs' / 'fixed' / 'marker'
        marker.write_text('preserve')
        with patch.object(runner.uuid, 'uuid4', return_value='fixed'):
            with self.assertRaises(FileExistsError):
                self.run_analysis()
        self.assertEqual(marker.read_text(), 'preserve')

    def test_optional_drift_success_and_failure(self):
        self.mock('prepare_drift_config', side_effect=lambda config: config)
        self.mock('bound_reference')
        original = self.spark_run
        for status in ('evaluated', 'insufficient_data', 'monitoring_failure'):
            with self.subTest(status=status):
                def spark(config, timeout, *, reserved):
                    result = original(config, timeout, reserved=reserved)
                    result['drift'] = dict(status=status, success=status == 'evaluated',
                                           evaluation_complete=status == 'evaluated')
                    return result
                self.spark.side_effect = spark
                report = self.run_analysis(drift_reference='reference.csv')
                if status == 'evaluated':
                    self.assertTrue(report['success'], report)
                else:
                    self.assert_failed(report, 'drift_verification')

    def test_reserved_supervisor_rejects_existing_evidence(self):
        for name in ('config.json', 'summary.json', 'checkpoint', 'batches', 'predictions'):
            with self.subTest(name=name):
                directory = self.root / name.replace('.', '_')
                directory.mkdir()
                (directory / name).touch()
                with self.assertRaisesRegex(ValueError, 'already used'):
                    supervise({'run_dir': str(directory)}, 1, reserved=True)

    def test_reserved_supervisor_launches_with_existing_metadata(self):
        directory = self.root / 'reserved'
        directory.mkdir()
        (directory / 'analysis.json').write_text('{}')
        config = dict(run_dir=str(directory), replay_id='replay', topic='flows',
                      expected_count=2, max_records=2)
        process = MagicMock()
        process.wait.return_value = 0
        def launch(*args, **kwargs):
            write_json(directory / 'summary.json', dict(success=True,
                       accounting_complete=True, errors=[]))
            return process
        with patch('src.streaming.spark_consumer.subprocess.Popen', side_effect=launch), \
                patch('src.streaming.spark_consumer.terminate_group'):
            self.assertTrue(supervise(config, 1, reserved=True)['success'])
        with self.assertRaises(FileExistsError):
            supervise(config, 1)

    def test_orchestrated_supervisor_preserves_public_summary(self):
        directory = self.root / 'reserved'
        directory.mkdir()
        public = dict(success=False, accounting_complete=False, errors=['not complete'])
        write_json(directory / 'summary.json', public)
        config = dict(run_dir=str(directory), replay_id='replay', topic='flows',
                      expected_count=2, max_records=2, orchestrated=True)
        process = MagicMock()
        process.wait.return_value = 0
        def launch(*args, **kwargs):
            write_json(summary_path(config), dict(success=True,
                       accounting_complete=True, errors=[]))
            return process
        with patch('src.streaming.spark_consumer.subprocess.Popen', side_effect=launch), \
                patch('src.streaming.spark_consumer.terminate_group'):
            self.assertTrue(supervise(config, 1, reserved=True)['success'])
        self.assertEqual(json.loads((directory / 'summary.json').read_text()), public)
        with self.assertRaisesRegex(ValueError, 'already used'):
            supervise(config, 1, reserved=True)

    def test_orchestrated_worker_writes_only_private_spark_summary(self):
        directory = self.root / 'worker'
        directory.mkdir()
        public = dict(success=False, accounting_complete=False)
        write_json(directory / 'summary.json', public)
        config = dict(run_dir=str(directory), replay_id='replay', topic='flows',
                      expected_count=2, max_records=2, orchestrated=True)
        spark = MagicMock()
        spark.version = SPARK_VERSION
        spark.sparkContext._jvm.scala.util.Properties.versionNumberString.return_value = '2.12.18'
        with patch('pyspark.__version__', SPARK_VERSION), \
                patch('pyspark.sql.SparkSession.builder') as builder, \
                patch('src.streaming.spark_consumer.execute_query'), \
                patch('src.streaming.spark_consumer.summarize', return_value=dict(success=True)):
            builder.master.return_value = builder
            builder.appName.return_value = builder
            builder.config.return_value = builder
            builder.getOrCreate.return_value = spark
            self.assertEqual(worker(config), 0)
        self.assertEqual(json.loads((directory / 'summary.json').read_text()), public)
        self.assertTrue(json.loads((directory / 'spark-summary.json').read_text())['success'])
        spark.stop.assert_called_once()


class TopicTests(unittest.TestCase):
    def test_creation_and_empty_partition_required(self):
        with patch('confluent_kafka.admin.AdminClient') as admin, \
                patch('confluent_kafka.Consumer') as consumer, \
                patch.object(runner, 'require_single_partition') as partition:
            consumer.return_value.get_watermark_offsets.return_value = (0, 0)
            runner.create_topic('localhost:9092', 'fresh', 2)
            admin.return_value.create_topics.return_value.__getitem__.return_value.result.assert_called_once_with(timeout=2)
            partition.assert_called_once_with('localhost:9092', 'fresh')
            consumer.return_value.close.assert_called_once()
            consumer.return_value.get_watermark_offsets.return_value = (0, 1)
            with self.assertRaisesRegex(ValueError, 'not empty'):
                runner.create_topic('localhost:9092', 'fresh', 2)


if __name__ == '__main__':
    unittest.main()
