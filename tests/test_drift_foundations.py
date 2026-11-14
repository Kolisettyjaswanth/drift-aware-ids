"""First drift-streaming increment; no Kafka, Spark or model artifacts needed."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.drift.ks import FEATURES
from src.producer.events import row_to_event
from src.streaming.drift_reference import load_reference
from src.streaming.drift_windows import plan_windows


def records(n, capture='evaluation.csv', start=0):
    result=[]
    for i in range(start,start+n):
        e=row_to_event({**{f:'1.5' for f in FEATURES}, 'Label':'BENIGN',
                        'SourceFile':capture},source_file='replay.csv',source_row=i,replay_id='r')
        result.append({'valid':True,'kafka':{'topic':'flows','partition':0,'offset':i+20},'event':e})
    return result


def plan(rows, **kw):
    return plan_windows(rows,replay_id='r',topic='flows',**kw)


def reference_text(n=100):
    return ','.join(FEATURES+['Label','SourceFile'])+'\n'+(
        ','.join(['1.5']*len(FEATURES)+['BENIGN','reference.csv'])+'\n')*n


class ReferenceTests(unittest.TestCase):
    def load(self,text,**kw):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'reference.csv';p.write_text(text,encoding='utf-8')
            return load_reference(p,**kw)

    def test_snapshot_and_fingerprint(self):
        text=reference_text()
        ref=self.load(text)
        self.assertEqual(ref.provenance['sha256'],hashlib.sha256(text.encode()).hexdigest())
        self.assertEqual(ref.provenance['rows'],100)
        self.assertEqual(ref.provenance['capture_counts'],{'reference.csv':100})
        self.assertEqual(list(ref.values),FEATURES)
        self.assertEqual(ref.values.iloc[0,0],1.5)

    def test_worker_fingerprint_binding(self):
        fingerprint=hashlib.sha256(reference_text().encode()).hexdigest()
        self.load(reference_text(),expected_sha256=fingerprint)
        with self.assertRaisesRegex(ValueError,'fingerprint'):
            self.load(reference_text().replace('1.5','2.5'),expected_sha256=fingerprint)

    def test_insufficient_rows(self):
        with self.assertRaises(ValueError): self.load(reference_text(99))

    def test_invalid_numeric_values(self):
        for value in ['','NaN','inf','-inf','invalid','1e999']:
            with self.subTest(value=value),self.assertRaises(ValueError):
                self.load(reference_text().replace('1.5',value,1))

    def test_bad_labels_or_capture(self):
        for text in [reference_text().replace('BENIGN','ATTACK',1),
                     reference_text().replace('reference.csv','',1)]:
            with self.assertRaises(ValueError): self.load(text)

    def test_duplicate_missing_headers_and_width(self):
        texts=['', reference_text().replace(FEATURES[0],FEATURES[1],1),
               reference_text().replace('SourceFile','missing',1),
               reference_text().replace('BENIGN,reference.csv','BENIGN',1)]
        for text in texts:
            with self.subTest(text=text[:50]),self.assertRaises(ValueError): self.load(text)

    def test_limits(self):
        with patch('src.streaming.drift_reference.MAX_REFERENCE_BYTES',10):
            with self.assertRaises(ValueError): self.load(reference_text())
        with patch('src.streaming.drift_reference.MAX_REFERENCE_ROWS',99):
            with self.assertRaises(ValueError): self.load(reference_text())

    def test_missing_file_and_encoding(self):
        with self.assertRaises(FileNotFoundError): load_reference('/no-such-ids-reference.csv')
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'invalid.csv';p.write_bytes(b'\xff')
            with self.assertRaises(ValueError): load_reference(p)


class WindowTests(unittest.TestCase):
    def test_cross_batch_snapshot_accumulation(self):
        rows=records(1000)
        partial=plan(rows[:400])
        self.assertEqual(partial.windows[0]['status'],'insufficient_data')
        final=plan(rows[:400]+rows[400:])
        self.assertEqual(len(final.windows),1)
        self.assertEqual(final.windows[0]['status'],'ready')
        self.assertEqual(len(final.windows[0]['membership']),1000)
        self.assertIsNone(final.windows[0]['alert'])

    def test_retry_and_unordered_microbatch_delivery(self):
        rows=records(1000)
        first=plan(rows)
        self.assertEqual(first,plan(list(reversed(rows))))
        self.assertEqual(first,plan(rows))
        self.assertEqual(len(first.windows),1)

    def test_multiple_nonoverlapping_windows(self):
        result=plan(records(2100),max_records=3000)
        self.assertEqual([w['records'] for w in result.windows],[1000,1000,100])
        ids=[m['event_id'] for w in result.windows for m in w['membership']]
        self.assertEqual(len(ids),len(set(ids)))

    def test_capture_boundaries_never_fill_across_captures(self):
        rows=records(600,'A')+records(1000,'B',start=600)+records(400,'A',start=1600)
        result=plan(rows,max_records=3000)
        self.assertEqual([w['records'] for w in result.windows],[600,1000,400])
        self.assertEqual([w['source_capture'] for w in result.windows],['A','B','A'])
        self.assertEqual(result.windows[0]['close_reason'],'capture_boundary')

    def test_multi_partition_rejected_before_global_sort(self):
        rows=records(2);rows[1]['kafka']['partition']=1
        result=plan(rows)
        self.assertEqual(result.status,'monitoring_failure')
        self.assertEqual(result.windows,[])
        self.assertIn('partition',result.error)

    def test_single_observed_nonzero_partition(self):
        rows=records(1);rows[0]['kafka']['partition']=3
        self.assertTrue(plan(rows).planning_complete) # observed ordering only; broker preflight is later

    def test_continuity_duplicate_row_and_offset(self):
        for change in ['gap','duplicate_row','duplicate_offset']:
            rows=records(3)
            if change=='gap': rows.pop(1)
            elif change=='duplicate_row':
                rows[1]['event']['source_row']=0;rows[1]['event']['event_id']='r:0'
            else: rows[1]['kafka']['offset']=rows[0]['kafka']['offset']
            self.assertFalse(plan(rows).planning_complete)

    def test_offset_gaps_allowed_source_rows_not_gapped(self):
        rows=records(2);rows[1]['kafka']['offset']+=10
        self.assertTrue(plan(rows).planning_complete)

    def test_unknown_capture_mixed_replay_and_file(self):
        for change in ['capture','replay','file']:
            rows=records(2)
            if change=='capture': rows[1]['event']['source_capture']=None
            elif change=='replay':
                rows[1]['event']['replay_id']='other';rows[1]['event']['event_id']='other:1'
            else: rows[1]['event']['source_file']='other.csv'
            self.assertFalse(plan(rows).planning_complete)

    def test_null_and_missing_monitored_features_not_imputed(self):
        rows=records(1000)
        rows[9]['event']['features'][FEATURES[0]]=None
        rows[9]['event']['invalid_features']=[FEATURES[0]]
        del rows[10]['event']['features'][FEATURES[1]]
        result=plan(rows)
        self.assertEqual(result.windows[0]['status'],'invalid_monitored_features')
        self.assertEqual(len(result.windows[0]['issues']),2)
        self.assertEqual(result.windows[0]['records'],1000)
        self.assertIsNone(result.windows[0]['alert'])

    def test_malformed_transport_and_schema_fail_explicitly(self):
        for change in ['transport','nan','coordinates','identity','topic']:
            rows=records(1)
            if change=='transport': rows[0]['valid']=False
            elif change=='nan': rows[0]['event']['features'][FEATURES[0]]=float('nan')
            elif change=='coordinates': rows[0]['kafka']['offset']=True
            elif change=='identity': rows[0]['event']['event_id']='wrong'
            else: rows[0]['kafka']['topic']='other'
            self.assertEqual(plan(rows).status,'monitoring_failure')

    def test_cap_and_empty(self):
        self.assertEqual(plan([]).status,'insufficient_data')
        self.assertFalse(plan(records(2),max_records=1).planning_complete)
        self.assertFalse(plan([],max_records=10001).planning_complete)

    def test_no_input_mutation_and_membership_json(self):
        rows=records(2);before=copy.deepcopy(rows)
        result=plan(rows)
        self.assertEqual(rows,before)
        json.dumps(result.windows,allow_nan=False)
        self.assertEqual(result.windows[0]['membership'][0]['offset'],20)

    def test_late_bad_record_prevents_partial_ready_claim(self):
        rows=records(1001);rows[-1]['event']['source_row']=1002
        rows[-1]['event']['event_id']='r:1002'
        result=plan(rows,max_records=2000)
        self.assertFalse(result.planning_complete);self.assertEqual(result.windows,[])


class FailurePathTests(unittest.TestCase):
    def test_reference_malformed_quotes(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'reference.csv'
            p.write_text(reference_text()+'"unfinished')
            with self.assertRaisesRegex(ValueError,'Malformed'):
                load_reference(p)

    def test_record_read_failure_is_explicit(self):
        def source():
            yield records(1)[0]
            raise OSError('failed to read committed batch')
        result=plan(source())
        self.assertEqual(result.status,'monitoring_failure')
        self.assertFalse(result.planning_complete)
        self.assertIn('failed to read',result.error)
        self.assertEqual(result.windows,[])
