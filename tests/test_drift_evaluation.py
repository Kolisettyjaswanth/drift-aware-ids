"""Offline KS execution/persistence tests using generated, labeled fixtures."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
from src.drift import ks
from src.streaming.drift_reference import load_reference
from src.streaming.drift_windows import plan_windows, WindowPlan
from src.streaming.drift_evaluation import (evaluate_plan, evaluate_and_persist,
                                            persist_report, EvidenceConflict)
from tests.test_drift_foundations import records, reference_text


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        path=self.root/'reference.csv';path.write_text(reference_text())
        self.reference=load_reference(path)

    def evaluate(self, rows):
        return evaluate_plan(self.reference,plan_windows(rows,replay_id='r',topic='flows',
                                                       max_records=10000),replay_id='r')


class EvaluationTests(Fixture):
    def test_real_detector_identical_window_and_direct_parity(self):
        rows=records(1000)
        report=self.evaluate(rows)
        self.assertTrue(report['success'])
        self.assertTrue(report['evaluation_complete'])
        window=report['windows'][0]
        direct=ks.detect_window(self.reference.values,pd.DataFrame(
            [{f:r['event']['features'][f] for f in ks.FEATURES} for r in rows]))
        self.assertEqual(window['ks_results'],direct)
        self.assertFalse(window['alert'])
        self.assertEqual(window['status'],'evaluated')
        self.assertEqual(report['reference']['sha256'],self.reference.provenance['sha256'])
        self.assertEqual(window['membership'][0]['source_row'],0)
        self.assertEqual(window['membership'][-1]['offset'],1019)
        self.assertEqual(window['source_capture'],'evaluation.csv')

    def test_real_detector_strong_shift_alerts(self):
        rows=records(1000)
        for row in rows:
            for feature in ks.FEATURES[:2]: row['event']['features'][feature]+=10
        report=self.evaluate(rows)
        self.assertTrue(report['success'])
        self.assertTrue(report['windows'][0]['alert'])
        self.assertEqual(report['windows'][0]['ks_results']['drifted_feature_count'],2)

    def test_partial_is_not_negative_drift(self):
        with patch.object(ks,'detect_window') as detector:
            report=self.evaluate(records(100))
        detector.assert_not_called()
        self.assertFalse(report['success'])
        self.assertEqual(report['status'],'insufficient_data')
        self.assertIsNone(report['windows'][0]['alert'])
        self.assertIsNone(report['windows'][0]['ks_results'])

    def test_empty_plan(self):
        with patch.object(ks,'detect_window') as detector:
            report=self.evaluate([])
        detector.assert_not_called()
        self.assertFalse(report['evaluation_complete'])
        self.assertEqual(report['status'],'insufficient_data')

    def test_invalid_monitored_values_skip_detector(self):
        rows=records(1000)
        rows[0]['event']['features'][ks.FEATURES[0]]=None
        rows[0]['event']['invalid_features']=[ks.FEATURES[0]]
        with patch.object(ks,'detect_window') as detector:
            report=self.evaluate(rows)
        detector.assert_not_called()
        self.assertEqual(report['status'],'invalid_monitored_features')
        self.assertFalse(report['success'])
        self.assertIsNone(report['windows'][0]['alert'])
        self.assertEqual(len(report['windows'][0]['issues']),1)

    def test_planning_failure_never_calls_detector(self):
        rows=records(1);rows[0]['valid']=False
        with patch.object(ks,'detect_window') as detector:
            report=self.evaluate(rows)
        detector.assert_not_called()
        self.assertEqual(report['status'],'monitoring_failure')
        self.assertFalse(report['success'])

    def test_planning_complete_is_not_execution_success(self):
        plan=plan_windows(records(10),replay_id='r',topic='flows')
        self.assertTrue(plan.planning_complete)
        result=evaluate_plan(self.reference,plan,replay_id='r')
        self.assertFalse(result['success'])
        self.assertFalse(result['evaluation_complete'])

    def test_detector_exception_is_explicit_failure(self):
        with patch.object(ks,'detect_window',side_effect=RuntimeError('simulated KS failure')):
            report=self.evaluate(records(1000))
        self.assertFalse(report['success'])
        self.assertEqual(report['status'],'monitoring_failure')
        self.assertEqual(report['windows'][0]['status'],'monitoring_failure')
        self.assertIsNone(report['windows'][0]['alert'])
        self.assertIn('simulated KS failure',report['errors'][0])

    def test_nonfinite_detector_result_fails(self):
        result=ks.detect_window(self.reference.values,self.reference.values.iloc[:0].reindex(
            range(1000),fill_value=1.5))
        result['features'][ks.FEATURES[0]]['p_value']=float('nan')
        with patch.object(ks,'detect_window',return_value=result):
            report=self.evaluate(records(1000))
        self.assertFalse(report['success']);self.assertIsNone(report['windows'][0]['alert'])

    def test_ready_flag_cannot_bypass_complete_check(self):
        plan=plan_windows(records(1),replay_id='r',topic='flows')
        plan.windows[0]['status']='ready'
        with patch.object(ks,'detect_window') as detector:
            report=evaluate_plan(self.reference,plan,replay_id='r')
        detector.assert_not_called()
        self.assertEqual(report['status'],'monitoring_failure')

    def test_mixed_evaluated_and_partial_not_complete(self):
        report=self.evaluate(records(1001))
        self.assertEqual([w['status'] for w in report['windows']],['evaluated','insufficient_data'])
        self.assertEqual(report['evaluated_windows'],1)
        self.assertFalse(report['success'])

    def test_one_detector_failure_cannot_be_hidden_by_later_success(self):
        real=ks.detect_window
        calls=[]
        def detector(*args,**kwargs):
            calls.append(1)
            if len(calls)==1:
                raise RuntimeError('first window failed')
            return real(*args,**kwargs)
        with patch.object(ks,'detect_window',side_effect=detector):
            report=self.evaluate(records(2000))
        self.assertEqual(len(calls),2)
        self.assertEqual([w['status'] for w in report['windows']],['monitoring_failure','evaluated'])
        self.assertFalse(report['success'])
        self.assertFalse(report['evaluation_complete'])
        self.assertEqual(report['evaluated_windows'],1)

    def test_reference_and_plan_inputs_not_mutated(self):
        plan=plan_windows(records(1000),replay_id='r',topic='flows')
        before=deepcopy(plan)
        values=self.reference.values.copy()
        evaluate_plan(self.reference,plan,replay_id='r')
        self.assertEqual(plan,before)
        pd.testing.assert_frame_equal(values,self.reference.values)

    def test_reference_metadata_and_replay_mismatch(self):
        plan=plan_windows(records(1000),replay_id='r',topic='flows')
        self.assertFalse(evaluate_plan(self.reference,plan,replay_id='other')['success'])
        bad=deepcopy(self.reference);bad.provenance['sha256']='fake'
        self.assertFalse(evaluate_plan(bad,plan,replay_id='r')['success'])


class PersistenceTests(Fixture):
    def prepared(self,n=1000):
        return plan_windows(records(n),replay_id='r',topic='flows',max_records=10000)

    def test_publish_then_identical_retry_is_one_file(self):
        plan=self.prepared()
        first=evaluate_and_persist(self.reference,plan,replay_id='r',run_dir=self.root)
        second=evaluate_and_persist(self.reference,plan,replay_id='r',run_dir=self.root)
        self.assertTrue(first['success']);self.assertTrue(second['success'])
        self.assertEqual(first['persistence']['status'],'persisted')
        self.assertEqual(second['persistence']['status'],'reused')
        directory=self.root/'drift'/'evaluations'
        self.assertEqual(len(list(directory.glob('*.json'))),1)
        self.assertEqual(list(directory.glob('*.tmp')),[])
        saved=json.loads(Path(first['persistence']['path']).read_text())
        self.assertEqual(saved,{k:v for k,v in first.items() if k!='persistence'})

    def test_inconsistent_result_cannot_overwrite(self):
        report=self.evaluate(records(1000))
        persisted=persist_report(self.root,report)
        path=Path(persisted['path']);before=path.read_bytes()
        changed=deepcopy(report);changed['windows'][0]['ks_results']['features'][ks.FEATURES[0]]['p_value']=.5
        with self.assertRaises(EvidenceConflict): persist_report(self.root,changed)
        self.assertEqual(path.read_bytes(),before)
        self.assertEqual(len(list(path.parent.glob('*.json'))),1)
        self.assertEqual(list(path.parent.glob('*.tmp')),[])

    def test_changed_input_has_distinct_auditable_identity(self):
        first=self.evaluate(records(1000))
        rows=records(1000);rows[0]['event']['features'][ks.FEATURES[0]]=2
        second=self.evaluate(rows)
        self.assertNotEqual(first['snapshot_id'],second['snapshot_id'])
        persist_report(self.root,first);persist_report(self.root,second)
        self.assertEqual(len(list((self.root/'drift'/'evaluations').glob('*.json'))),2)

    def test_failure_evidence_preserves_transport_and_predictions(self):
        for name in ['batches','predictions']:
            (self.root/name).mkdir();(self.root/name/'batch-0.jsonl').write_text('original evidence\n')
        with patch.object(ks,'detect_window',side_effect=RuntimeError('KS failed')):
            report=evaluate_and_persist(self.reference,self.prepared(),replay_id='r',run_dir=self.root)
        self.assertFalse(report['success'])
        self.assertEqual(report['status'],'monitoring_failure')
        self.assertEqual(report['persistence']['status'],'persisted')
        for name in ['batches','predictions']:
            self.assertEqual((self.root/name/'batch-0.jsonl').read_text(),'original evidence\n')
        self.assertIn('KS failed',Path(report['persistence']['path']).read_text())

    def test_partial_and_invalid_outcomes_persist_without_alert(self):
        result=evaluate_and_persist(self.reference,self.prepared(10),replay_id='r',run_dir=self.root)
        saved=json.loads(Path(result['persistence']['path']).read_text())
        self.assertFalse(saved['success']);self.assertIsNone(saved['windows'][0]['alert'])
        self.assertEqual(saved['status'],'insufficient_data')

    def test_invalid_features_persist_with_no_false_decision(self):
        rows=records(1000)
        rows[0]['event']['features'][ks.FEATURES[0]]=None
        rows[0]['event']['invalid_features']=[ks.FEATURES[0]]
        plan=plan_windows(rows,replay_id='r',topic='flows')
        report=evaluate_and_persist(self.reference,plan,replay_id='r',run_dir=self.root)
        saved=json.loads(Path(report['persistence']['path']).read_text())
        self.assertEqual(saved['status'],'invalid_monitored_features')
        self.assertFalse(saved['success'])
        self.assertIsNone(saved['windows'][0]['alert'])
        self.assertIsNone(saved['windows'][0]['ks_results'])

    def test_persistence_failure_cannot_report_success(self):
        with patch('src.streaming.drift_evaluation.os.link',side_effect=OSError('disk failure')):
            report=evaluate_and_persist(self.reference,self.prepared(),replay_id='r',run_dir=self.root)
        self.assertFalse(report['success'])
        self.assertEqual(report['status'],'monitoring_failure')
        self.assertEqual(report['failure_stage'],'persistence')
        self.assertEqual(list((self.root/'drift'/'evaluations').glob('*')),[])

    def test_conflict_wrapper_cannot_report_success(self):
        plan=self.prepared()
        first=evaluate_and_persist(self.reference,plan,replay_id='r',run_dir=self.root)
        path=Path(first['persistence']['path']);before=path.read_bytes()
        with patch.object(ks,'detect_window',side_effect=RuntimeError('transient failure')):
            retry=evaluate_and_persist(self.reference,plan,replay_id='r',run_dir=self.root)
        self.assertFalse(retry['success'])
        self.assertEqual(retry['persistence']['status'],'monitoring_failure')
        self.assertEqual(path.read_bytes(),before)

    def test_concurrent_identical_publication(self):
        report=self.evaluate(records(1000))
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:persist_report(self.root,report),range(2)))
        self.assertEqual(sorted(r['status'] for r in results),['persisted','reused'])
        self.assertEqual(len(list((self.root/'drift'/'evaluations').glob('*.json'))),1)

    def test_run_directory_and_snapshot_required(self):
        report=self.evaluate(records(1000))
        with self.assertRaises(ValueError): persist_report(self.root/'missing',report)
        report['snapshot_id']='../bad'
        with self.assertRaises(ValueError): persist_report(self.root,report)

    def test_planning_failure_evidence(self):
        plan=WindowPlan([],status='monitoring_failure',error='offset order invalid')
        result=evaluate_and_persist(self.reference,plan,replay_id='r',run_dir=self.root)
        self.assertFalse(result['success'])
        self.assertEqual(result['planning_error'],'offset order invalid')
        self.assertEqual(result['persistence']['status'],'persisted')
