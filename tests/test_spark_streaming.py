"""Broker/JVM-independent tests for Spark acceptance logic."""
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch
from src.producer.events import row_to_event, encode_event
from src.streaming.validation import (source_options, validate_record, summarize,
                                     read_records, CONNECTOR)
from src.streaming.spark_consumer import (BatchSink, execute_query, supervise, worker,
                                         write_json)
from src.streaming.verify_output import verify


def row(index=0, replay='r', offset=None):
    event=row_to_event({'Average Packet Size':'1.25','large':'9007199254740993',
                        'empty':'NaN','Label':'BENIGN','SourceFile':'Monday.csv'},
                       source_file='input.csv',source_row=index,replay_id=replay)
    return dict(key=event['event_id'].encode(), value=encode_event(event),
                topic='flows',partition=0,offset=index if offset is None else offset)


class Frame:
    def __init__(self,rows): self.rows=rows
    def toLocalIterator(self,**kw): return iter(self.rows)


class ParsingTests(unittest.TestCase):
    def test_schema_preservation(self):
        record=validate_record(row())
        self.assertTrue(record['valid'])
        self.assertEqual(record['event']['features']['large'],9007199254740993)
        self.assertIsNone(record['event']['features']['empty'])
        self.assertEqual(record['event']['source_capture'],'Monday.csv')

    def test_malformed_tombstone_key_and_schema(self):
        for change in [dict(value=None),dict(value=b'{'),dict(value=b'null'),
                       dict(key=None),dict(key=b'wrong'),dict(key=b'\xff'),
                       dict(value=b'{"schema_version":2}')]:
            with self.subTest(change=change):
                r=row();r.update(change)
                self.assertFalse(validate_record(r)['valid'])

    def test_config(self):
        options=source_options('127.0.0.1:9092','flows',100)
        self.assertEqual(options['startingOffsets'],'earliest')
        self.assertEqual(options['failOnDataLoss'],'true')
        self.assertNotIn('kafka.group.id',options)
        self.assertTrue(CONNECTOR.endswith('_2.12:3.5.7'))
        for args in [('', 'flows',100),('x','bad topic',100),('x','flows',0),('x','flows',1001)]:
            with self.assertRaises(ValueError): source_options(*args)


class AccountingTests(unittest.TestCase):
    def stats(self,rows,n=2):
        return summarize([validate_record(r) for r in rows],'r',n,'flows',10)

    def test_exact_ids_not_just_count(self):
        self.assertTrue(self.stats([row(0),row(1)])['success'])
        s=self.stats([row(1),row(2)])
        self.assertFalse(s['success']);self.assertEqual(s['missing_expected'],1)
        self.assertEqual(s['unexpected_rows'],1)

    def test_duplicates_at_different_offsets(self):
        s=self.stats([row(0),row(0,offset=1)])
        self.assertFalse(s['success']);self.assertEqual(s['duplicates'],1)

    def test_repeated_kafka_offset(self):
        s=self.stats([row(0),row(1,offset=0)])
        self.assertFalse(s['success']);self.assertEqual(s['repeated_offsets'],1)

    def test_old_replay_cannot_pass(self):
        s=self.stats([row(0),row(1,replay='old')])
        self.assertFalse(s['success']);self.assertEqual(s['unrelated'],1)

    def test_extra_invalid_empty_and_missing(self):
        invalid=row();invalid['value']=None
        for rows in [[],[row(0)],[row(0),row(1),row(2)],[row(0),invalid]]:
            self.assertFalse(self.stats(rows)['success'])

    def test_safety_cap(self):
        with self.assertRaises(ValueError):
            summarize([validate_record(row(i)) for i in range(3)],'r',2,'flows',2)
        for replay,n,cap in [('',2,10),('r',0,10),('r',11,10),('r',2,10001)]:
            with self.assertRaises(ValueError): summarize([],replay,n,'flows',cap)


class SinkTests(unittest.TestCase):
    def test_batch_retry_replaces_instead_of_double_counting(self):
        with tempfile.TemporaryDirectory() as d:
            sink=BatchSink(d,10)
            frame=Frame([row(0),row(1)])
            sink(frame,0);sink(frame,0)
            records=list(read_records(d))
            self.assertEqual(len(records),2)
            self.assertTrue(summarize(records,'r',2,'flows',10)['success'])

    def test_failed_partial_batch_not_committed(self):
        with tempfile.TemporaryDirectory() as d:
            sink=BatchSink(d,1)
            with self.assertRaises(ValueError): sink(Frame([row(0),row(1)]),0)
            self.assertEqual(list(read_records(d)),[])
            self.assertEqual(list((Path(d)/'batches').glob('*.tmp')),[])

    def test_invalid_records_are_persisted_and_counted(self):
        with tempfile.TemporaryDirectory() as d:
            r=row();r['value']=b'bad'
            BatchSink(d,10)(Frame([r]),0)
            stats=summarize(read_records(d),'r',1,'flows',10)
            self.assertEqual(stats['invalid'],1)
            self.assertEqual(stats['consumed'],1)
            self.assertFalse(stats['success'])

    def test_source_feature_comparison(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            source=root/'input.csv'
            source.write_text('Average Packet Size,large,empty,Label,SourceFile\n1.25,9007199254740993,NaN,BENIGN,Monday.csv\n')
            config=dict(replay_id='r',expected_count=1,topic='flows',max_records=10)
            write_json(root/'config.json',config)
            write_json(root/'summary.json',dict(success=True,accounting_complete=True))
            BatchSink(d,10)(Frame([row()]),0)
            self.assertTrue(verify(d,source)['success'])
            source.write_text(source.read_text().replace('1.25','2.25'))
            with self.assertRaises(ValueError): verify(d,source)


class LifecycleTests(unittest.TestCase):
    def config(self,root):
        return dict(run_dir=str(Path(root)/'new'),brokers='127.0.0.1:9092',topic='flows',
                    replay_id='r',expected_count=1,max_records=10,batch_size=100)

    def test_query_failure_stops_active_query(self):
        spark=MagicMock()
        query=MagicMock();query.isActive=True
        query.awaitTermination.side_effect=RuntimeError('query failed')
        stream=spark.readStream.format.return_value.options.return_value.load.return_value
        writer=stream.select.return_value.writeStream.foreachBatch.return_value
        writer.option.return_value.trigger.return_value.start.return_value=query
        with tempfile.TemporaryDirectory() as d:
            cfg=self.config(d);Path(cfg['run_dir']).mkdir()
            with self.assertRaises(RuntimeError): execute_query(spark,cfg)
        query.stop.assert_called_once()
        writer.option.return_value.trigger.assert_called_once_with(availableNow=True)

    def test_existing_run_directory_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            cfg=self.config(d);Path(cfg['run_dir']).mkdir()
            with self.assertRaises(FileExistsError): supervise(cfg,1)

    def test_timeout_cannot_report_success(self):
        with tempfile.TemporaryDirectory() as d:
            cfg=self.config(d)
            process=MagicMock();process.wait.side_effect=subprocess.TimeoutExpired('spark',1)
            with patch('src.streaming.spark_consumer.subprocess.Popen',return_value=process), patch(
                    'src.streaming.spark_consumer.terminate_group') as stop:
                report=supervise(cfg,1)
            self.assertFalse(report['success']);self.assertFalse(report['accounting_complete'])
            stop.assert_called_once_with(process)
            self.assertIn('timeout',report['errors'][0])

    def test_missing_runtime_is_nonzero_report(self):
        with tempfile.TemporaryDirectory() as d:
            cfg=self.config(d);Path(cfg['run_dir']).mkdir()
            with patch.dict('sys.modules',{'pyspark':SimpleNamespace(__version__='wrong')}):
                self.assertEqual(worker(cfg),1)
            report=json.loads((Path(cfg['run_dir'])/'summary.json').read_text())
            self.assertFalse(report['success'])
            self.assertIn('Expected PySpark',report['errors'][0])

    def test_supervisor_success_comes_from_worker_report(self):
        with tempfile.TemporaryDirectory() as d:
            cfg=self.config(d)
            process=MagicMock();process.wait.return_value=0
            def launch(*args,**kw):
                write_json(Path(cfg['run_dir'])/'summary.json',dict(success=True,accounting_complete=True))
                return process
            with patch('src.streaming.spark_consumer.subprocess.Popen',side_effect=launch), patch(
                    'src.streaming.spark_consumer.terminate_group'):
                self.assertTrue(supervise(cfg,1)['success'])

class CleanupTests(unittest.TestCase):
    def test_worker_query_error_releases_session(self):
        spark=MagicMock();spark.version='3.5.7'
        spark.sparkContext._jvm.scala.util.Properties.versionNumberString.return_value='2.12.18'
        builder=MagicMock();builder.master.return_value=builder
        builder.appName.return_value=builder;builder.config.return_value=builder
        builder.getOrCreate.return_value=spark
        modules={'pyspark':SimpleNamespace(__version__='3.5.7'),
                 'pyspark.sql':SimpleNamespace(SparkSession=SimpleNamespace(builder=builder))}
        with tempfile.TemporaryDirectory() as d:
            cfg=dict(run_dir=d,brokers='x',topic='flows',replay_id='r',
                     expected_count=1,max_records=10,batch_size=100)
            with patch.dict('sys.modules',modules), patch(
                    'src.streaming.spark_consumer.execute_query',side_effect=RuntimeError('query failed')):
                self.assertEqual(worker(cfg),1)
            spark.stop.assert_called_once()

    def test_process_group_termination(self):
        import os
        import sys
        from src.streaming.spark_consumer import terminate_group
        if os.name!='posix':
            self.skipTest('WSL/Linux process-group test')
        process=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],
                                 start_new_session=True)
        try:
            terminate_group(process)
            self.assertIsNotNone(process.poll())
        finally:
            if process.poll() is None:
                process.kill();process.wait()
