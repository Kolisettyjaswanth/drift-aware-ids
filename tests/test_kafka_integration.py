"""Opt-in broker roundtrip using generated CSV, never starts Docker."""
import os
from pathlib import Path
import tempfile
import unittest
import uuid


@unittest.skipUnless(os.environ.get('IDS_KAFKA_INTEGRATION') == '1',
                     'Kafka roundtrip requires explicit IDS_KAFKA_INTEGRATION=1 and running broker')
class KafkaIntegrationTests(unittest.TestCase):
    def test_csv_broker_roundtrip(self):
        from confluent_kafka import Producer, Consumer
        from confluent_kafka.admin import AdminClient, NewTopic
        from src.producer.events import csv_events
        from src.producer.kafka_config import producer_config, consumer_config
        from src.producer.replay import publish
        from src.producer.verify_consumer import consume
        brokers=os.environ.get('IDS_KAFKA_BROKERS','localhost:9092')
        replay_id=str(uuid.uuid4())
        topic='ids-test-'+replay_id
        admin=AdminClient({'bootstrap.servers':brokers})
        created=False
        try:
            admin.create_topics([NewTopic(topic,num_partitions=1,replication_factor=1)],
                                request_timeout=15)[topic].result(timeout=20)
            created=True
            with tempfile.TemporaryDirectory() as d:
                path=Path(d)/'fixture.csv'
                path.write_text('Destination Port,Average Packet Size,Label\n'+
                                '22,12.5,BENIGN\n'*10)
                result=publish(Producer(producer_config(brokers,topic)),
                               csv_events(path,replay_id),topic,max_events=10,rate=0)
                self.assertTrue(result['success'],result)
                self.assertEqual(result['delivered'],10)
                verified=consume(Consumer(consumer_config(brokers,topic,'verify-'+replay_id)),
                                 topic,max_events=10,timeout=45,replay_id=replay_id)
                self.assertTrue(verified['success'],verified)
                self.assertEqual(verified['unique_matched'],10)
        finally:
            if created:
                admin.delete_topics([topic],request_timeout=15)[topic].result(timeout=20)
