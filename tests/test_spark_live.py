"""Opt-in Kafka+Spark tests. Creates only UUID-scoped disposable topics."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid


@unittest.skipUnless(os.environ.get('IDS_SPARK_INTEGRATION')=='1',
                     'Live Spark/Kafka test requires IDS_SPARK_INTEGRATION=1 and local dependencies')
class SparkKafkaIntegrationTests(unittest.TestCase):
    def test_valid_replay_and_explicit_malformed_failure(self):
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic
        from src.producer.events import csv_events
        from src.producer.kafka_config import producer_config
        from src.producer.replay import publish
        from src.streaming.verify_output import verify
        brokers=os.environ.get('IDS_KAFKA_BROKERS','127.0.0.1:9092')
        admin=AdminClient({'bootstrap.servers':brokers})
        for malformed in (False,True):
            replay_id=str(uuid.uuid4())
            topic='ids-spark-test-'+replay_id
            created=False
            try:
                admin.create_topics([NewTopic(topic,1,1)],request_timeout=15)[topic].result(timeout=20)
                created=True
                with tempfile.TemporaryDirectory() as d:
                    source=Path(d)/'fixture.csv'
                    source.write_text('Destination Port,Average Packet Size,Label\n'+'22,1.25,BENIGN\n'*100)
                    producer=Producer(producer_config(brokers,topic))
                    if malformed:
                        failed=[]
                        producer.produce(topic,key=b'bad',value=b'{',
                                         on_delivery=lambda err,msg: failed.append(err) if err else None)
                        self.assertEqual(producer.flush(35),0)
                        self.assertFalse(failed)
                        count=1
                    else:
                        result=publish(producer,csv_events(source,replay_id),topic,max_events=100,rate=0)
                        self.assertTrue(result['success'],result)
                        count=100
                    run_dir=Path(d)/'spark-run'
                    process=subprocess.run([sys.executable,'-m','src.streaming.spark_consumer',
                        '--brokers',brokers,'--topic',topic,'--replay-id',replay_id,
                        '--expected-count',str(count),'--run-dir',str(run_dir),'--timeout','180'],
                        capture_output=True,text=True,timeout=205)
                    report=json.loads((run_dir/'summary.json').read_text())
                    self.assertTrue(report['accounting_complete'],(report,process.stderr[-3000:]))
                    self.assertEqual(report['consumed'],count)
                    if malformed:
                        self.assertNotEqual(process.returncode,0)
                        self.assertEqual(report['invalid'],1)
                        self.assertFalse(report['success'])
                    else:
                        self.assertEqual(process.returncode,0,(report,process.stderr[-3000:]))
                        self.assertTrue(report['success'])
                        self.assertTrue(verify(run_dir,source)['success'])
            finally:
                if created:
                    admin.delete_topics([topic],request_timeout=15)[topic].result(timeout=20)
