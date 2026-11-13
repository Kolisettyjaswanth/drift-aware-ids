import unittest
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import ks_2samp
from src.drift.ks import detect_window, compare_feature, legacy_ks_2samp
from src.drift.correlation import correlations


class DetectorTests(unittest.TestCase):
    def setUp(self):
        self.ref = pd.DataFrame({'a': np.linspace(0, 1, 1000),
                                 'b': np.linspace(0, 1, 1000)})

    def detect(self, frame, **kw):
        return detect_window(self.ref, frame, features=['a', 'b'], **kw)

    def test_identical(self):
        self.assertFalse(self.detect(self.ref)['alert'])

    def test_two_feature_rule(self):
        one = self.ref.copy(); one['a'] += 10
        r = self.detect(one)
        self.assertEqual(r['drifted_features'], ['a'])
        self.assertFalse(r['alert'])
        one['b'] += 10
        self.assertTrue(self.detect(one)['alert'])

    def test_missing_column(self):
        with self.assertRaises(ValueError): self.detect(self.ref[['a']])

    def test_nonfinite(self):
        for value in [np.nan, np.inf, -np.inf]:
            bad = self.ref.copy(); bad.loc[0, 'a'] = value
            with self.assertRaises(ValueError): self.detect(bad)

    def test_non_numeric(self):
        with self.assertRaises(ValueError): compare_feature(['bad']*100, range(100))

    def test_incomplete(self):
        for n in [0, 99, 999]:
            with self.assertRaises(ValueError): self.detect(self.ref.iloc[:n])

    def test_oversized(self):
        with self.assertRaises(ValueError): self.detect(pd.concat([self.ref]*2))

    def test_short_reference(self):
        with self.assertRaises(ValueError):
            detect_window(self.ref.iloc[:99], self.ref, features=['a', 'b'])

    def test_invalid_settings(self):
        for kw in [{'alpha': 0}, {'min_ks': -1}, {'minimum_features': 3},
                   {'minimum_features': 0}]:
            with self.assertRaises(ValueError): self.detect(self.ref, **kw)

    def test_drop_policy(self):
        r = compare_feature([np.nan]+list(range(100)), range(100), invalid='drop')
        self.assertEqual(r['ks_statistic'], 0)
        with self.assertRaises(ValueError):
            compare_feature([np.nan]*100, range(100), invalid='drop')

    def test_legacy_equivalence(self):
        for method in ['auto', 'asymp']:
            expected = ks_2samp(self.ref.a, self.ref.b+1, method=method)
            self.assertEqual(legacy_ks_2samp(self.ref.a, self.ref.b+1, method), tuple(expected))


class CorrelationTests(unittest.TestCase):
    def test_known_residual_correlation(self):
        p=np.tile([0., 1.], 10)
        e=np.repeat(np.linspace(-.1,.1,10),2)
        r=correlations(.2+.5*p+e, .2+.5*p-e, p)
        self.assertGreater(r['raw'], .8)
        self.assertAlmostEqual(r['prevalence_adjusted'], -1)

    def test_degenerate(self):
        self.assertIsNone(correlations([.5]*10, [.5]*10, [.5]*10)['raw'])
        r=correlations(np.linspace(0,1,10), np.linspace(0,1,10), np.linspace(0,1,10))
        self.assertIsNone(r['prevalence_adjusted'])

    def test_bad_inputs(self):
        for args in [([0]*3, [0]*3, [0]*3), ([np.nan]*4,[0]*4,[0]*4),
                     ([2]*4,[0]*4,[0]*4), ([0]*4,[0]*5,[0]*4)]:
            with self.assertRaises(ValueError): correlations(*args)


class DatasetTests(unittest.TestCase):
    @unittest.skipUnless(Path('data/processed/drift/reference.csv').exists() and
                         Path('data/processed/drift/heldout_portscan_predictions.csv').exists(),
                         'CIC-IDS2017 reference and held-out predictions unavailable')
    def test_real_window(self):
        from src.drift.ks import FEATURES
        ref=pd.read_csv('data/processed/drift/reference.csv')
        pred=pd.read_csv('data/processed/drift/heldout_portscan_predictions.csv')
        self.assertGreaterEqual(len(pred), 1000)
        detect_window(ref, pred.iloc[:1000], features=FEATURES)


if __name__ == '__main__': unittest.main()

class BoundaryTests(unittest.TestCase):
    def test_threshold_boundaries(self):
        from src.drift.ks import feature_alert
        self.assertTrue(feature_alert(.1, .004, alpha=.005, min_ks=.1))
        self.assertFalse(feature_alert(.1, .005, alpha=.005, min_ks=.1))
        self.assertFalse(feature_alert(.099, .001, alpha=.005, min_ks=.1))

    def test_constant_prevalence(self):
        r=correlations(np.linspace(0,1,10),np.linspace(0,1,10),[.5]*10)
        self.assertAlmostEqual(r['raw'], r['prevalence_adjusted'])

    def test_existing_experiment_window(self):
        from src.drift.evaluate_broad_reference import evaluate_capture, FEATURES
        rng=np.random.default_rng(99)
        ref=pd.DataFrame(rng.normal(size=(1000,10)),columns=FEATURES)
        rows=evaluate_capture(ref,ref.copy())
        self.assertEqual(len(rows),1)
        self.assertFalse(rows[0]['alert'])
