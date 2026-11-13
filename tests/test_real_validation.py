"""Fixtures test the validator, never establish CIC-IDS2017 acceptance."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pandas as pd
from src.drift.ks import FEATURES, detect_window
from src.drift.validate_real import (load_capture, split_samples, paired_scenarios,
                                     run_validation, frame_hash, SETTINGS)


def fixture(rows=1200):
    rng=np.random.default_rng(91)
    frame=pd.DataFrame(rng.uniform(1, 2, (rows, len(FEATURES))), columns=FEATURES)
    frame['Label']='BENIGN'
    frame.index.name='source_row'
    return frame


class RealValidationTests(unittest.TestCase):
    def test_disjoint_and_unique(self):
        ref, test=split_samples(fixture(), 0, 200, 5, 100)
        self.assertTrue(ref.index.intersection(test.index).empty)
        self.assertTrue(test.index.is_unique)
        self.assertEqual(len(test), 500)

    def test_reproducible_and_seed_sensitive(self):
        frame=fixture()
        a,b=split_samples(frame, 0, 200, 5, 100)
        c,d=split_samples(frame, 0, 200, 5, 100)
        self.assertEqual(frame_hash(a),frame_hash(c))
        self.assertEqual(frame_hash(b),frame_hash(d))
        e,_=split_samples(frame, 1, 200, 5, 100)
        self.assertNotEqual(frame_hash(a),frame_hash(e))

    def test_pairing_and_no_mutation(self):
        ref, window=split_samples(fixture(), 0, 200, 1, 100)
        before=frame_hash(window)
        scenarios,_=paired_scenarios(ref,window)
        self.assertEqual(frame_hash(window),before)
        for sample in scenarios.values():
            self.assertTrue(sample.index.equals(window.index))
            self.assertTrue(sample.Label.equals(window.Label))
            self.assertEqual(sample.Label.ne('BENIGN').mean(),0)
        self.assertEqual(frame_hash(scenarios['unchanged']),before)

    def test_strict_detector_strong_shifts(self):
        ref, window=split_samples(fixture(), 0, 200, 1, 100)
        scenarios,_=paired_scenarios(ref,window)
        for name in ('location_shift','scale_shift'):
            self.assertTrue(detect_window(ref,scenarios[name],window_size=100)['alert'])

    def test_all_scenarios_share_thresholds_and_aggregate_gate(self):
        seen=[]
        def detector(ref, window, **kw):
            seen.append(kw)
            return {'alert': False}
        settings=dict(SETTINGS,window_size=100)
        with patch('src.drift.validate_real.detect_window', side_effect=detector):
            a=run_validation(fixture(), {}, reference_size=100, windows=1, settings=settings)
            b=run_validation(fixture(), {}, reference_size=100, windows=1, settings=settings)
        self.assertTrue(all(k==settings for k in seen))
        self.assertEqual(a,b)
        self.assertFalse(a['passed']) # no synthetic shift detections must fail
        self.assertEqual(a['aggregate']['unchanged']['windows'],5)

    def test_insufficient_and_duplicate_id(self):
        with self.assertRaises(ValueError): split_samples(fixture(10),0)
        bad=fixture();bad.index=[0]*len(bad)
        with self.assertRaises(ValueError): split_samples(bad,0)
        with self.assertRaises(ValueError):
            run_validation(fixture(),{},seeds=(0,1))

    def test_loader_filter_and_provenance(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'Monday.csv'
            frame=fixture();frame.loc[0,FEATURES[0]]=np.inf
            frame.to_csv(path,index=False)
            loaded,meta=load_capture(path)
            self.assertEqual(meta['excluded_nonfinite_rows'],1)
            self.assertNotIn(0,loaded.index)
            self.assertEqual(len(meta['file_sha256']),64)
            frame.loc[1,'Label']='PortScan';frame.to_csv(path,index=False)
            with self.assertRaises(ValueError): load_capture(path)

    def test_loader_missing_feature(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'bad.csv';fixture().drop(columns=FEATURES[0]).to_csv(path,index=False)
            with self.assertRaises(ValueError): load_capture(path)


class AcceptanceBoundaryTests(unittest.TestCase):
    def test_exact_rate_thresholds(self):
        from src.drift.validate_real import acceptance_passed
        a={'unchanged': {'rate': .05}, 'location_shift': {'rate': .90},
           'scale_shift': {'rate': .90}}
        self.assertTrue(acceptance_passed(a))
        a['unchanged']['rate']=.05001
        self.assertFalse(acceptance_passed(a))
        a['unchanged']['rate']=0
        a['scale_shift']['rate']=.89999
        self.assertFalse(acceptance_passed(a))
