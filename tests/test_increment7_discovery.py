"""Chronological evidence discovery, without HTTP threads, Kafka, or Spark."""
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from backend.app.main import list_runs


class RunDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        env = patch.dict(os.environ, {'IDS_EVIDENCE_ROOT': str(self.root)})
        env.start()
        self.addCleanup(env.stop)

    def run_fixture(self, name, created_at=None, *, raw=None, file_time=100):
        directory = self.root / name
        directory.mkdir()
        summary = directory / 'summary.json'
        summary.write_text(raw if raw is not None else json.dumps(
            dict(success=False, accounting_complete=False, created_at=created_at)))
        os.utime(summary, (file_time, file_time))
        return directory

    def names(self, limit=100):
        result = list_runs(limit=limit)
        self.assertEqual(set(result), {'runs', 'count', 'limit'})
        self.assertEqual(result['count'], len(result['runs']))
        self.assertEqual(result['limit'], limit)
        self.assertTrue(all(set(item) == {'run_id', 'summary_present'} and
                            item['summary_present'] is True for item in result['runs']))
        return [item['run_id'] for item in result['runs']]

    def test_newest_of_101_uuid_runs_is_included_at_limit_100(self):
        newest = '00000000-0000-4000-8000-000000000000'
        for index in range(100):
            self.run_fixture(f'ffffffff-ffff-4fff-8fff-{index:012x}',
                             '2026-11-15T10:00:00+00:00', file_time=9999999999)
        self.run_fixture(newest, '2026-11-16T10:00:00+00:00', file_time=1)
        names = self.names()
        self.assertEqual(len(names), 100)
        self.assertEqual(names[0], newest)

    def test_equal_timestamps_use_deterministic_descending_run_id(self):
        for name in ('run-b', 'run-a', 'run-c'):
            self.run_fixture(name, '2026-11-16T10:00:00+00:00')
        self.assertEqual(self.names(), ['run-c', 'run-b', 'run-a'])
        self.assertEqual(self.names(limit=2), ['run-c', 'run-b'])

    def test_aware_timestamp_offsets_are_normalized(self):
        self.run_fixture('older', '2026-11-16T15:00:00+05:30')
        self.run_fixture('newer', '2026-11-16T10:00:00Z')
        self.assertEqual(self.names(), ['newer', 'older'])

    def test_summary_updates_do_not_reorder_creation_times(self):
        older = self.run_fixture('older', '2026-11-15T10:00:00+00:00')
        self.run_fixture('newer', '2026-11-16T10:00:00+00:00')
        os.utime(older / 'summary.json', (9999999999, 9999999999))
        self.assertEqual(self.names(), ['newer', 'older'])

    def test_missing_or_malformed_metadata_falls_back_to_config_time(self):
        malformed = ('{}', '{broken', '[]', '{"created_at": 42}',
                     '{"created_at": "invalid"}',
                     '{"created_at": "2026-11-16T10:00:00"}',
                     '[' * 2000 + '0' + ']' * 2000,
                     'x' * (1024 * 1024 + 1))
        for index, raw in enumerate(malformed):
            directory = self.run_fixture(f'legacy-{index}', raw=raw, file_time=999999)
            config = directory / 'config.json'
            config.write_text('{}')
            os.utime(config, (index + 100, index + 100))
        self.assertEqual(self.names(), [f'legacy-{i}' for i in reversed(range(len(malformed)))])

    def test_missing_config_falls_back_to_summary_time_with_stable_ties(self):
        self.run_fixture('older', raw='{}', file_time=100)
        self.run_fixture('newer-a', raw='{broken', file_time=200)
        self.run_fixture('newer-b', raw='[]', file_time=200)
        self.assertEqual(self.names(), ['newer-b', 'newer-a', 'older'])

    def test_symlink_config_is_not_used_for_fallback(self):
        older = self.run_fixture('older', file_time=100)
        newer = self.run_fixture('newer', file_time=200)
        (older / 'config.json').symlink_to(newer / 'summary.json')
        self.assertEqual(self.names(), ['newer', 'older'])


if __name__ == '__main__':
    unittest.main()
