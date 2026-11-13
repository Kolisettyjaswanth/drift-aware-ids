import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.producer.events import (row_to_event, csv_events, encode_event, decode_event,
                                 validate_event)
from src.producer.kafka_config import producer_config, consumer_config
from src.producer.replay import publish
from src.producer.verify_consumer import consume


def event(index=0):
    return row_to_event({' Destination Port': '22', 'Average Packet Size': '1.25',
                         'Label': 'BENIGN', 'SourceFile': 'Monday.csv'},
                        source_file='capture.csv', source_row=index, replay_id='test')


class Producer:
    def __init__(self, fail=False, pending=False):
        self.messages=[]; self.callbacks=[]; self.fail=fail; self.pending=pending
        self.flushed=False
    def list_topics(self, **kw):
        return SimpleNamespace(topics={'flows': SimpleNamespace(error=None)})
    def produce(self, topic, **kw):
        self.messages.append(kw)
        self.callbacks.append(kw['on_delivery'])
    def poll(self, timeout):
        if not self.pending:
            while self.callbacks:
                self.callbacks.pop(0)('delivery error' if self.fail else None, None)
    def flush(self, timeout):
        self.flushed=True; self.poll(0)
        return len(self.callbacks)


class Message:
    def __init__(self, value, error=None): self.payload=value; self.err=error
    def value(self): return self.payload
    def error(self): return self.err


class Consumer:
    def __init__(self, messages): self.messages=iter(messages); self.closed=False
    def list_topics(self, **kw):
        return SimpleNamespace(topics={'flows': SimpleNamespace(error=None)})
    def subscribe(self, topics): self.topics=topics
    def poll(self, timeout): return next(self.messages, None)
    def close(self): self.closed=True


class EventTests(unittest.TestCase):
    def test_round_trip(self):
        e=event(); self.assertEqual(decode_event(encode_event(e)),e)
        self.assertEqual(e['features']['Average Packet Size'],1.25)
        self.assertNotIn('Label',e['features'])
        self.assertNotIn('SourceFile',e['features'])

    def test_missing_nonfinite_nonnumeric(self):
        e=row_to_event(dict(a='',b='NaN',c='Infinity',d='-inf',e='bad',f='9007199254740993'),
                       source_file='x',source_row=0,replay_id='r')
        self.assertEqual(e['features']['f'],9007199254740993)
        self.assertEqual(set(e['invalid_features']),set('abcde'))
        self.assertIsNone(e['label'])
        self.assertNotIn(b'NaN',encode_event(e))

    def test_csv_headers_metadata_and_bounded_iteration(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'capture.csv'
            p.write_text('\ufeff Destination Port ,Average Packet Size, Label,SourceFile\n22,1.5,BENIGN,Monday.csv\nbad,width\n')
            rows=csv_events(p,'r'); e=next(rows)
            self.assertEqual(e['features'],{'Destination Port':22,'Average Packet Size':1.5})
            self.assertEqual(e['source_capture'],'Monday.csv')
            with self.assertRaises(ValueError): next(rows)

    def test_csv_rejects_bad_headers_and_training_input(self):
        for contents in ['', 'a,a,Label\n1,2,BENIGN\n',
                         'a,Target,Label\n1,0,BENIGN\n', 'a\n1\n']:
            with self.subTest(contents=contents), tempfile.TemporaryDirectory() as d:
                p=Path(d)/'x.csv';p.write_text(contents)
                with self.assertRaises(ValueError): list(csv_events(p,'r'))

    def test_schema_rejections(self):
        for change in [dict(schema_version=True),dict(source_row=-1),dict(event_id='wrong'),
                       dict(replayed_at='2026-01-01'),dict(features={}),
                       dict(features={'a':True}),dict(features={'a':float('inf')}),
                       dict(invalid_features=['missing']),dict(label=42),dict(source_capture='')]:
            with self.subTest(change=change):
                e=event();e.update(change)
                with self.assertRaises(ValueError): validate_event(e)

    def test_malformed_json(self):
        for value in [b'null', b'[]',b'{',b'\xff',None,b'{"x":NaN}',b'{"x":1,"x":2}']:
            with self.subTest(value=value):
                with self.assertRaises(ValueError): decode_event(value)


class ConfigTests(unittest.TestCase):
    def test_safe_defaults(self):
        p=producer_config('localhost:9092','flows')
        self.assertTrue(p['enable.idempotence']);self.assertEqual(p['acks'],'all')
        self.assertLessEqual(p['queue.buffering.max.kbytes'],16384)
        c=consumer_config('localhost:9092','flows','verify')
        self.assertFalse(c['enable.auto.commit'])
        self.assertFalse(c['allow.auto.create.topics'])

    def test_invalid(self):
        for broker,topic in [('', 'flows'),('x', '..'),('x','bad topic')]:
            with self.assertRaises(ValueError): producer_config(broker,topic)
        with self.assertRaises(ValueError): consumer_config('x','flows','')


class PublishingTests(unittest.TestCase):
    def test_bounded_does_not_read_extra_row(self):
        def rows():
            yield event()
            raise AssertionError('Read beyond requested bound')
        p=Producer();s=publish(p,rows(),'flows',max_events=1,rate=0)
        self.assertTrue(s['success']);self.assertEqual(s['delivered'],1)
        self.assertEqual(s['enqueued'],1);self.assertTrue(p.flushed)

    def test_delivery_failure_not_counted_as_success(self):
        p=Producer(fail=True);s=publish(p,[event()],'flows',rate=0)
        self.assertFalse(s['success']);self.assertEqual(s['delivered'],0)
        self.assertEqual(s['delivery_failed'],1)

    def test_flush_timeout_reports_unconfirmed(self):
        s=publish(Producer(pending=True),[event()],'flows',rate=0)
        self.assertFalse(s['success']);self.assertEqual(s['unconfirmed'],1)

    def test_queue_backpressure_recovers(self):
        p=Producer();real=p.produce
        attempts=[]
        def send(*args, **kw):
            attempts.append(1)
            if len(attempts)==1:
                raise BufferError()
            return real(*args, **kw)
        with patch.object(p,'produce',side_effect=send):
            s=publish(p,[event()],'flows',rate=0)
        self.assertEqual(len(attempts),2)
        self.assertEqual(s['enqueued'],1)
        self.assertEqual(s['delivered'],1)
        self.assertTrue(s['success'])

    def test_acknowledgements_dispatched_by_flush(self):
        p=Producer()
        def flush(timeout):
            while p.callbacks:
                p.callbacks.pop(0)(None,None)
            return 0
        with patch.object(p,'poll',return_value=None), patch.object(p,'flush',side_effect=flush):
            s=publish(p,[event()],'flows',rate=0)
        self.assertTrue(s['success']);self.assertEqual(s['delivered'],1)

    def test_missing_topic_fails_without_publishing(self):
        p=Producer()
        with patch.object(p,'list_topics',return_value=SimpleNamespace(topics={})):
            s=publish(p,[event()],'flows',rate=0)
        self.assertFalse(s['success']);self.assertEqual(s['enqueued'],0)

    def test_queue_timeout(self):
        p=Producer()
        with patch.object(p,'produce',side_effect=BufferError()), patch(
                'src.producer.replay.time.monotonic',side_effect=[0,0,100]):
            s=publish(p,[event()],'flows',rate=0,timeout=1)
        self.assertFalse(s['success']);self.assertEqual(s['enqueued'],0)

    def test_source_error_flushes_prior_deliveries(self):
        def rows():
            yield event()
            raise ValueError('bad CSV')
        p=Producer();s=publish(p,rows(),'flows',rate=0)
        self.assertFalse(s['success']);self.assertEqual(s['delivered'],1)
        self.assertTrue(p.flushed)

    def test_connection_and_shutdown_errors(self):
        p=Producer()
        with patch.object(p,'list_topics',side_effect=RuntimeError('offline')):
            s=publish(p,[event()],'flows',rate=0)
        self.assertFalse(s['success']);self.assertEqual(s['attempted'],0)
        self.assertTrue(p.flushed)
        with patch.object(p,'flush',side_effect=RuntimeError('flush failed')):
            self.assertFalse(publish(p,[],'flows',rate=0)['success'])

    def test_invalid_limits(self):
        for kw in [dict(max_events=0),dict(rate=-1),dict(rate=float('nan')),dict(timeout=0)]:
            with self.assertRaises(ValueError): publish(Producer(),[],'flows',**kw)

    def test_interrupt_flushes(self):
        p=Producer()
        with patch.object(p,'poll',side_effect=KeyboardInterrupt()), patch.object(p,'flush',return_value=1):
            s=publish(p,[event()],'flows',rate=0)
        self.assertFalse(s['success']);self.assertTrue(s['interrupted'])

    def test_pacing(self):
        with patch('src.producer.replay.time.sleep') as sleep:
            publish(Producer(),[event()],'flows',rate=100)
        self.assertEqual(sleep.call_count,1)


class ConsumingTests(unittest.TestCase):
    def test_valid_count_and_close(self):
        c=Consumer([Message(encode_event(event(i))) for i in range(2)])
        s=consume(c,'flows',max_events=2,replay_id='test')
        self.assertTrue(s['success']);self.assertEqual(s['unique_matched'],2)
        self.assertTrue(c.closed)

    def test_malformed_tombstone_and_mismatch(self):
        for payload in [b'bad',None]:
            c=Consumer([Message(payload)])
            s=consume(c,'flows',max_events=1)
            self.assertFalse(s['success']);self.assertEqual(s['malformed'],1)
        s=consume(Consumer([Message(encode_event(event()))]),'flows',max_events=1,replay_id='other')
        self.assertFalse(s['success']);self.assertEqual(s['matched'],0)

    def test_duplicate_detection(self):
        c=Consumer([Message(encode_event(event()))]*2)
        s=consume(c,'flows',max_events=2)
        self.assertFalse(s['success']);self.assertEqual(s['duplicates'],1)

    def test_broker_error_closes(self):
        c=Consumer([Message(None,'broker error')])
        self.assertFalse(consume(c,'flows',max_events=1)['success'])
        self.assertTrue(c.closed)

    def test_timeout_no_false_pass(self):
        c=Consumer([])
        with patch('src.producer.verify_consumer.time.monotonic',side_effect=[0,100]):
            s=consume(c,'flows',max_events=1,timeout=1)
        self.assertFalse(s['success']);self.assertTrue(c.closed)

    def test_consumer_exceptions_and_close_failure(self):
        c=Consumer([])
        with patch.object(c,'subscribe',side_effect=RuntimeError('failed')):
            self.assertFalse(consume(c,'flows',max_events=1)['success'])
        with patch.object(c,'close',side_effect=RuntimeError('close failed')):
            c.messages=iter([Message(encode_event(event()))])
            self.assertFalse(consume(c,'flows',max_events=1)['success'])

    def test_consumer_interrupt(self):
        c=Consumer([])
        with patch.object(c,'poll',side_effect=KeyboardInterrupt()):
            s=consume(c,'flows',max_events=1)
        self.assertTrue(s['interrupted']);self.assertTrue(c.closed)

    def test_invalid_limits(self):
        for kw in [dict(max_events=0),dict(timeout=float('inf'))]:
            with self.assertRaises(ValueError): consume(Consumer([]),'flows',**kw)

class MockRoundtripTests(unittest.TestCase):
    def test_csv_to_published_bytes_to_verifier(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'flows.csv'
            path.write_text(' Destination Port , Average Packet Size , Label,SourceFile\n22,1.25,BENIGN,Monday.csv\n80,Infinity,PortScan,Friday.csv\n')
            p=Producer()
            result=publish(p,csv_events(path,'roundtrip'),'flows',max_events=2,rate=0)
            self.assertTrue(result['success'])
            self.assertEqual(result['delivered'],2)
            records=[decode_event(m['value']) for m in p.messages]
            self.assertEqual(records[0]['features']['Destination Port'],22)
            self.assertIsNone(records[1]['features']['Average Packet Size'])
            c=Consumer([Message(m['value']) for m in p.messages])
            checked=consume(c,'flows',max_events=2,replay_id='roundtrip')
            self.assertTrue(checked['success'])
