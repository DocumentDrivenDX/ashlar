from __future__ import annotations

import ast
import unittest
from pathlib import Path

import run_catalog_view_reason_r2 as supervisor


class CatalogViewReasonR2SupervisorControls(unittest.TestCase):
    def test_approval_binds_one_attempt_and_r1_postattempt_state(self):
        payload = supervisor.build_approval_payload()
        self.assertEqual(payload['limits'], {
            'attempts': 1,
            'automatic_retries': 0,
            'child_deadline_seconds': 120,
            'combined_retained_and_attempt_bytes': 1073741824,
            'log_bytes': 16777216,
            'parallelism': 'sequential local only',
            'cloud_resources': False,
        })
        self.assertEqual(payload['retained_state']['post_attempt_tree_sha256'], supervisor.DATA_TREE_SHA256)
        self.assertEqual(payload['retained_state']['post_attempt_bytes'], 63999633)
        self.assertEqual(payload['inputs']['r1_diagnostic_execution_result_sha256'], supervisor.R1_EXECUTION_RESULT_SHA256)
        self.assertEqual(payload['retained_state']['cold_backup_path'], str(supervisor.BACKUP))
        self.assertEqual(payload['read_boundary']['helper_runtime_distribution'], 'truss-toolkit==0.0.48')
        self.assertNotEqual(str(supervisor.BACKUP), '/private/tmp/ashlar-truss050-current-source-2eeecf40-r15/catalog-view-reason-r1/cold-backup/pgdata')

    def test_supervisor_has_no_full_migration_worker_or_effectful_later_stages(self):
        source = Path(supervisor.__file__).read_text()
        self.assertNotIn('campaign_worker_r15', source)
        self.assertNotIn('run_migration', source)
        self.assertIn("[str(OLD48_VENV / 'bin/python'), '-I', '-B', str(WORKER)]", source)
        self.assertEqual(supervisor.WORKER.name, 'read_catalog_view_reason_r2.py')

    def test_exact_approval_is_checked_before_attempt_marker_and_postgres_start(self):
        source = Path(supervisor.__file__).read_text()
        self.assertLess(source.index('if approval != expected:'), source.index('ATTEMPT.mkdir('))
        execute_source = source[source.index('def execute()'):]
        self.assertLess(execute_source.index('verify_helper_runtime_metadata()'), execute_source.index('ATTEMPT.mkdir('))
        self.assertLess(source.index('write_new(STARTED,'), source.index('from truss import LocalPostgres'))
        self.assertFalse(supervisor.ATTEMPT.exists())
        self.assertFalse(supervisor.APPROVAL.exists())
        self.assertFalse(supervisor.pg_owned())

    def test_supervisor_preflights_actual_helper_distribution_and_api_isolated(self):
        supervisor.verify_helper_runtime_metadata()

    def test_readonly_auxiliaries_and_current_state_match_the_bound_r1_result(self):
        supervisor.verify_runtime_auxiliaries()
        manifest = supervisor.tree_manifest(supervisor.DATA)
        self.assertEqual((manifest['tree_sha256'], manifest['bytes'], manifest['file_count']), (
            supervisor.DATA_TREE_SHA256, supervisor.DATA_BYTES, supervisor.DATA_FILE_COUNT,
        ))


if __name__ == '__main__':
    unittest.main()
