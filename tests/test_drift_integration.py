"""Offline increment-3 orchestration tests; no broker, JVM or model is fabricated."""
from contextlib import ExitStack
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from src.drift import ks
from src.streaming.drift_evaluation import evaluate_plan
from src.streaming.drift_windows import plan_windows
from src.streaming.drift_monitor import (DriftMonitor, DriftMonitoringError,
    prepare_drift_config, bound_reference, require_single_partition)
from src.streaming.spark_consumer import BatchSink, worker, supervise, main, merge_accounting
from src.streaming.validation import read_records
from src.producer.events import encode_event
from tests.test_drift_foundations import records, reference_text
from tests.test_spark_streaming import Frame
from tests.test_inference_sink import engine


def kafka_rows(n,shift=False):
    result=[]
    for record in records(n):
        event=record['event']
        if shift:
            for f in ks.FEATURES[:2]: event['features'][f]+=10
        result.append({**record['kafka'],'key':event['event_id'].encode(),'value':encode_event(event)})
    return result


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.source=self.root/'reference.csv';self.source.write_text(reference_text())

    def config(self,folder='run',**kw):
        return dict(run_dir=str(self.root/folder),brokers='127.0.0.1:9092',topic='flows',
                    replay_id='r',expected_count=1000,max_records=1000,batch_size=100,
                    enable_drift=kw.pop('enable_drift',True),
                    drift_reference=str(self.source),enable_inference=False,**kw)

    def prepared(self):
        return prepare_drift_config(self.config())


class ContractTests(Fixture):
    def test_reject_contradictory_results(self):
        config=self.prepared();reference=bound_reference(config)
        rows=records(1000)
        for r in rows:
            for f in ks.FEATURES[:2]: r['event']['features'][f]+=10
        plan=plan_windows(rows,replay_id='r',topic='flows')
        baseline=evaluate_plan(reference,plan,replay_id='r')['windows'][0]['ks_results']
        candidates=[]
        result=deepcopy(baseline);result['features'][ks.FEATURES[0]]['drift_detected']=False;candidates.append(result)
        result=deepcopy(baseline);result['drifted_feature_count']=1;candidates.append(result)
        result=deepcopy(baseline);result['drifted_feature_count']=True;candidates.append(result)
        result=deepcopy(baseline);result['drifted_features']=[];candidates.append(result)
        result=deepcopy(baseline);result['drifted_features']*=2;candidates.append(result)
        result=deepcopy(baseline);result['alert']=False;candidates.append(result)
        for result in candidates:
            with self.subTest(result=result),patch.object(ks,'detect_window',return_value=result):
                report=evaluate_plan(reference,plan,replay_id='r')
                self.assertFalse(report['success'])
                self.assertEqual(report['status'],'monitoring_failure')
                self.assertIsNone(report['windows'][0]['alert'])

    def test_worker_reference_binding(self):
        cfg=self.prepared()
        bound_reference(cfg)
        self.source.write_text(reference_text().replace('1.5','2.5'))
        with self.assertRaisesRegex(ValueError,'fingerprint'): bound_reference(cfg)
        cfg.pop('drift_reference_sha256')
        with self.assertRaisesRegex(ValueError,'preflight'): bound_reference(cfg)

    def test_metadata_requires_one_configured_partition(self):
        for count in (0,1,2):
            partitions={i:SimpleNamespace(error=None) for i in range(count)}
            client=MagicMock();client.list_topics.return_value=SimpleNamespace(
                topics={'flows':SimpleNamespace(error=None,partitions=partitions)})
            modules={'confluent_kafka':SimpleNamespace(),
                     'confluent_kafka.admin':SimpleNamespace(AdminClient=lambda conf:client)}
            with patch.dict('sys.modules',modules):
                if count==1: require_single_partition('x','flows')
                else:
                    with self.assertRaises(DriftMonitoringError): require_single_partition('x','flows')

    def test_metadata_missing_topic_errors_and_connection_failure(self):
        for answer in [SimpleNamespace(topics={}),
                       SimpleNamespace(topics={'flows':SimpleNamespace(error='topic error')}),
                       SimpleNamespace(topics={'flows':SimpleNamespace(error=None,
                           partitions={0:SimpleNamespace(error='partition error')})})]:
            client=MagicMock();client.list_topics.return_value=answer
            with patch.dict('sys.modules',{'confluent_kafka':SimpleNamespace(),
                 'confluent_kafka.admin':SimpleNamespace(AdminClient=lambda conf:client)}):
                with self.assertRaises(DriftMonitoringError): require_single_partition('x','flows')
        client.list_topics.side_effect=RuntimeError('broker down')
        with patch.dict('sys.modules',{'confluent_kafka':SimpleNamespace(),
             'confluent_kafka.admin':SimpleNamespace(AdminClient=lambda conf:client)}):
            with self.assertRaisesRegex(RuntimeError,'broker down'): require_single_partition('x','flows')


class SinkIntegrationTests(Fixture):
    def make_sink(self,*,inference=False):
        cfg=self.prepared();Path(cfg['run_dir']).mkdir()
        monitor=DriftMonitor(cfg,bound_reference(cfg))
        return BatchSink(cfg['run_dir'],1000,inference_engine=engine() if inference else None,
                         drift_monitor=monitor),monitor,cfg

    def test_cross_batches_retry_and_both_alert_decisions(self):
        for shifted in (False,True):
            with self.subTest(shifted=shifted),tempfile.TemporaryDirectory() as d:
                cfg=self.prepared();cfg['run_dir']=d
                monitor=DriftMonitor(cfg,bound_reference(cfg));sink=BatchSink(d,1000,drift_monitor=monitor)
                rows=kafka_rows(1000,shift=shifted)
                with patch('src.streaming.drift_monitor.require_single_partition'):
                    sink(Frame(rows[:400]),0)
                    self.assertEqual(monitor.summary()['status'],'insufficient_data')
                    sink(Frame(rows[400:]),1)
                    first=list(monitor.summary()['evidence_paths'])
                    sink(Frame(rows[400:]),1)
                    summary=monitor.finalize()
                self.assertTrue(summary['success']);self.assertEqual(summary['evaluated_windows'],1)
                self.assertEqual(summary['windows'][0]['alert'],shifted)
                self.assertEqual(summary['evidence_paths'],first)
                self.assertEqual(len(list(read_records(d))),1000)

    def test_combined_mode_preserves_predictions_on_detector_failure(self):
        sink,monitor,cfg=self.make_sink(inference=True)
        with patch('src.streaming.drift_monitor.require_single_partition'),patch.object(
                ks,'detect_window',side_effect=RuntimeError('KS failed')):
            with self.assertRaises(DriftMonitoringError): sink(Frame(kafka_rows(1000)),0)
        root=Path(cfg['run_dir'])
        self.assertEqual(len(list(read_records(root))),1000)
        path=next((root/'predictions').glob('*.jsonl'))
        self.assertEqual(len(path.read_text().splitlines()),1000)
        self.assertFalse(monitor.summary()['success'])

    def test_combined_mode_success_with_actual_ks_and_fake_inference(self):
        sink,monitor,cfg=self.make_sink(inference=True)
        with patch('src.streaming.drift_monitor.require_single_partition'):
            sink(Frame(kafka_rows(1000)),0)
            self.assertTrue(monitor.finalize()['success'])
        predictions=next((Path(cfg['run_dir'])/'predictions').glob('*.jsonl'))
        self.assertEqual(len(predictions.read_text().splitlines()),1000)

    def test_partial_finalization_fails_without_no_drift_claim(self):
        sink,monitor,cfg=self.make_sink()
        with patch('src.streaming.drift_monitor.require_single_partition'):
            sink(Frame(kafka_rows(10)),0)
            with self.assertRaisesRegex(DriftMonitoringError,'incomplete'): monitor.finalize()
        self.assertFalse(monitor.summary()['success'])
        self.assertIsNone(monitor.summary()['windows'][0]['alert'])

    def test_incomplete_finalize_failure_is_sticky_after_window_completes(self):
        sink,monitor,cfg=self.make_sink()
        first_batch=kafka_rows(10)
        remaining=[]
        for record in records(990,start=10):
            event=record['event']
            remaining.append({**record['kafka'],'key':event['event_id'].encode(),
                              'value':encode_event(event)})

        with patch('src.streaming.drift_monitor.require_single_partition'):
            sink(Frame(first_batch),0)
            with self.assertRaises(DriftMonitoringError):
                monitor.finalize()
            self.assertEqual(monitor.summary()['status'],'insufficient_data')

            sink.drift_monitor=None
            sink(Frame(remaining),1)
            self.assertEqual(len(list(read_records(cfg['run_dir']))),1000)
            with self.assertRaises(DriftMonitoringError):
                monitor.finalize()

    def test_invalid_feature_fails_after_transport_commit(self):
        sink,monitor,cfg=self.make_sink()
        rows=kafka_rows(1)
        event=json.loads(rows[0]['value']);event['features'][ks.FEATURES[0]]=None
        event['invalid_features']=[ks.FEATURES[0]];rows[0]['value']=encode_event(event)
        with patch('src.streaming.drift_monitor.require_single_partition'):
            with self.assertRaises(DriftMonitoringError): sink(Frame(rows),0)
        self.assertEqual(len(list(read_records(cfg['run_dir']))),1)
        self.assertFalse(monitor.summary()['success'])

    def test_metadata_failure_after_commit_is_sticky(self):
        sink,monitor,cfg=self.make_sink()
        with patch('src.streaming.drift_monitor.require_single_partition',side_effect=RuntimeError('broker down')):
            with self.assertRaises(DriftMonitoringError): sink(Frame(kafka_rows(1)),0)
        with patch('src.streaming.drift_monitor.require_single_partition') as check:
            with self.assertRaises(DriftMonitoringError): monitor.update()
        check.assert_not_called()
        self.assertEqual(len(list(read_records(cfg['run_dir']))),1)
        self.assertFalse(monitor.summary()['success'])

    def test_partition_count_change_during_updates_is_sticky(self):
        sink,monitor,cfg=self.make_sink()
        sink.drift_monitor=None
        sink(Frame(kafka_rows(1)),0)

        def metadata(partition_count):
            partitions={i:SimpleNamespace(error=None) for i in range(partition_count)}
            return SimpleNamespace(topics={'flows':SimpleNamespace(error=None,
                                                                   partitions=partitions)})

        client=MagicMock()
        client.list_topics.side_effect=[metadata(1),metadata(2),metadata(1)]
        modules={'confluent_kafka':SimpleNamespace(),
                 'confluent_kafka.admin':SimpleNamespace(AdminClient=lambda conf:client)}

        with patch.dict('sys.modules',modules):
            monitor.update()
            with self.assertRaisesRegex(DriftMonitoringError,'exactly one configured Kafka partition'):
                monitor.update()
            with self.assertRaisesRegex(DriftMonitoringError,'exactly one configured Kafka partition'):
                monitor.update()

        self.assertEqual(client.list_topics.call_count,2)
        summary=monitor.summary()
        self.assertFalse(summary['success'])
        self.assertIn('Drift requires exactly one configured Kafka partition',summary['errors'])

    def test_monitor_interruption_is_not_success(self):
        sink,monitor,cfg=self.make_sink()
        with patch('src.streaming.drift_monitor.require_single_partition',side_effect=KeyboardInterrupt()):
            with self.assertRaises(KeyboardInterrupt): sink(Frame(kafka_rows(1)),0)
        self.assertFalse(monitor.summary()['success'])
        self.assertEqual(monitor.summary()['status'],'monitoring_failure')
        self.assertEqual(len(list(read_records(cfg['run_dir']))),1)

    def test_persistence_failure_cannot_pass(self):
        sink,monitor,cfg=self.make_sink()
        with patch('src.streaming.drift_monitor.require_single_partition'),patch(
                'src.streaming.drift_evaluation.os.link',side_effect=OSError('disk full')):
            with self.assertRaises(DriftMonitoringError): sink(Frame(kafka_rows(1000)),0)
        self.assertFalse(monitor.summary()['success'])
        self.assertEqual(len(list(read_records(cfg['run_dir']))),1000)

    def test_safety_cap_still_applies(self):
        sink,monitor,cfg=self.make_sink()
        with patch('src.streaming.drift_monitor.require_single_partition') as check:
            with self.assertRaisesRegex(ValueError,'safety cap'): sink(Frame(kafka_rows(1001)),0)
        check.assert_not_called();self.assertEqual(list(read_records(cfg['run_dir'])),[])


class WorkerAndCliTests(Fixture):
    def spark_modules(self):
        spark=MagicMock();spark.version='3.5.7'
        spark.sparkContext._jvm.scala.util.Properties.versionNumberString.return_value='2.12.18'
        builder=MagicMock();builder.master.return_value=builder;builder.appName.return_value=builder
        builder.config.return_value=builder;builder.getOrCreate.return_value=spark
        return spark,{'pyspark':SimpleNamespace(__version__='3.5.7'),
                      'pyspark.sql':SimpleNamespace(SparkSession=SimpleNamespace(builder=builder))}

    def test_four_worker_modes(self):
        for inference,drift in ((False,False),(True,False),(False,True),(True,True)):
            with self.subTest(inference=inference,drift=drift),tempfile.TemporaryDirectory() as d:
                cfg=self.prepared();cfg.update(run_dir=d,enable_inference=inference,enable_drift=drift)
                spark,modules=self.spark_modules()
                monitor=MagicMock();monitor.summary.return_value=dict(success=True,status='evaluated',evaluation_complete=True)
                monitor.finalize.return_value=monitor.summary.return_value
                with ExitStack() as stack:
                    stack.enter_context(patch.dict('sys.modules',modules))
                    query=stack.enter_context(patch('src.streaming.spark_consumer.execute_query'))
                    stack.enter_context(patch('src.streaming.spark_consumer.summarize',return_value={'success':True}))
                    verify=stack.enter_context(patch('src.streaming.verify_inference.verify_inference',return_value={'verified':True}))
                    metadata=stack.enter_context(patch('src.streaming.drift_monitor.require_single_partition'))
                    stack.enter_context(patch('src.streaming.drift_monitor.DriftMonitor',return_value=monitor))
                    self.assertEqual(worker(cfg),0)
                self.assertEqual(verify.call_count,int(inference))
                self.assertEqual(metadata.call_count,int(drift))
                self.assertEqual('drift_monitor' in query.call_args.kwargs,drift)
                report=json.loads((Path(d)/'summary.json').read_text())
                self.assertEqual('drift' in report,drift)
                spark.stop.assert_called_once()

    def test_incomplete_monitoring_cannot_report_worker_success(self):
        cfg=self.prepared();Path(cfg['run_dir']).mkdir();spark,modules=self.spark_modules()
        monitor=MagicMock()
        monitor.finalize.side_effect=DriftMonitoringError('incomplete drift windows')
        monitor.summary.return_value=dict(success=False,status='insufficient_data',evaluation_complete=False)
        with patch.dict('sys.modules',modules),patch(
                'src.streaming.drift_monitor.require_single_partition'),patch(
                'src.streaming.drift_monitor.DriftMonitor',return_value=monitor),patch(
                'src.streaming.spark_consumer.execute_query'),patch(
                'src.streaming.spark_consumer.summarize',return_value={'success':True}):
            self.assertEqual(worker(cfg),1)
        report=json.loads((Path(cfg['run_dir'])/'summary.json').read_text())
        self.assertFalse(report['success'])
        self.assertEqual(report['drift']['status'],'insufficient_data')
        self.assertTrue(report['accounting_complete']) # transport done; drift incomplete
        self.assertIn('incomplete drift',report['errors'][0])

    def test_reference_mismatch_before_spark_start(self):
        cfg=self.prepared();Path(cfg['run_dir']).mkdir()
        self.source.write_text(reference_text().replace('1.5','2.5'))
        spark,modules=self.spark_modules()
        with patch.dict('sys.modules',modules):
            self.assertEqual(worker(cfg),1)
        spark.stop.assert_not_called()
        report=json.loads((Path(cfg['run_dir'])/'summary.json').read_text())
        self.assertFalse(report['success']);self.assertEqual(report['drift']['status'],'monitoring_failure')
        self.assertIn('fingerprint',report['errors'][0])

    def test_metadata_failure_before_spark_start(self):
        cfg=self.prepared();Path(cfg['run_dir']).mkdir()
        spark,modules=self.spark_modules()
        with patch.dict('sys.modules',modules),patch(
                'src.streaming.drift_monitor.require_single_partition',side_effect=RuntimeError('metadata failed')):
            self.assertEqual(worker(cfg),1)
        spark.stop.assert_not_called()
        report=json.loads((Path(cfg['run_dir'])/'summary.json').read_text())
        self.assertIn('metadata failed',report['errors'][0]);self.assertFalse(report['drift']['success'])

    def test_fallback_cannot_erase_errors_or_monitoring_failure(self):
        cfg=self.prepared();Path(cfg['run_dir']).mkdir();spark,modules=self.spark_modules()
        monitor=MagicMock();monitor.summary.return_value=dict(success=False,status='monitoring_failure',errors=['KS failed'])
        with patch.dict('sys.modules',modules),patch(
                'src.streaming.drift_monitor.require_single_partition'),patch(
                'src.streaming.drift_monitor.DriftMonitor',return_value=monitor),patch(
                'src.streaming.spark_consumer.execute_query',side_effect=RuntimeError('original failure')),patch(
                'src.streaming.spark_consumer.summarize',return_value={'success':True,'errors':[],'drift':{'success':True}}):
            self.assertEqual(worker(cfg),1)
        report=json.loads((Path(cfg['run_dir'])/'summary.json').read_text())
        self.assertIn('original failure',report['errors'][0]);self.assertFalse(report['drift']['success'])

    def test_cli_modes_and_missing_reference(self):
        for flags,enabled in [([],False),(['--enable-inference'],False),
                              (['--enable-drift','--drift-reference',str(self.source)],True),
                              (['--enable-inference','--enable-drift','--drift-reference',str(self.source)],True)]:
            with patch('sys.argv',['spark','--topic','flows','--replay-id','r']+flags),patch(
                    'src.streaming.spark_consumer.supervise',return_value={'success':True}) as run,patch('sys.stdout',io.StringIO()):
                self.assertEqual(main(),0)
            self.assertEqual(run.call_args.args[0]['enable_drift'],enabled)
        for flags in [['--enable-drift'],['--drift-reference',str(self.source)]]:
            with patch('sys.argv',['spark','--topic','flows','--replay-id','r']+flags),patch(
                    'src.streaming.spark_consumer.supervise') as run,patch('sys.stdout',io.StringIO()):
                self.assertEqual(main(),1)
            run.assert_not_called()

    def test_supervisor_preflight_failure_does_not_launch(self):
        cfg=self.config();cfg['drift_reference']=str(self.root/'missing.csv')
        with patch('src.streaming.spark_consumer.subprocess.Popen') as launch:
            report=supervise(cfg,1)
        launch.assert_not_called();self.assertFalse(report['success'])
        self.assertEqual(report['drift']['status'],'monitoring_failure')
        self.assertTrue((Path(cfg['run_dir'])/'summary.json').exists())

    def test_supervisor_passes_fingerprint_to_child(self):
        cfg=self.config();process=MagicMock();process.wait.return_value=0
        def launch(*args,**kwargs):
            root=Path(cfg['run_dir']);saved=json.loads((root/'config.json').read_text())
            self.assertEqual(bound_reference(saved).provenance['sha256'],saved['drift_reference_sha256'])
            (root/'summary.json').write_text(json.dumps({'success':True,'errors':[],
                                                       'drift':{'success':True,'status':'evaluated','evaluation_complete':True}}))
            return process
        with patch('src.streaming.spark_consumer.subprocess.Popen',side_effect=launch),patch(
                'src.streaming.spark_consumer.terminate_group'):
            self.assertTrue(supervise(cfg,1)['success'])

    def test_accounting_merge_preserves_existing_errors(self):
        report={'success':False,'errors':['first'],'drift':{'success':False},'accounting_complete':False}
        merge_accounting(report,{'success':True,'errors':[],'drift':{},'accounting_complete':True},partial=True)
        self.assertEqual(report['errors'],['first']);self.assertFalse(report['success'])
        self.assertFalse(report['drift']['success']);self.assertFalse(report['accounting_complete'])

    def test_supervisor_rejects_inconsistent_child_success(self):
        cfg=self.config();process=MagicMock();process.wait.return_value=0
        def launch(*args,**kwargs):
            (Path(cfg['run_dir'])/'summary.json').write_text(json.dumps(
                {'success':True,'errors':[],'drift':{'success':True,
                 'status':'insufficient_data','evaluation_complete':False}}))
            return process
        with patch('src.streaming.spark_consumer.subprocess.Popen',side_effect=launch),patch(
                'src.streaming.spark_consumer.terminate_group'):
            self.assertFalse(supervise(cfg,1)['success'])
