"""Effect-free closing Delta heads and Spark identity gate."""
import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).with_name('prepare.py')
SPEC = importlib.util.spec_from_file_location('g03_native_prepare_closing', SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ClosingSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
        self.heads={role:{'uuid':'uuid-'+role,'version':2,'logHeadSha256':'sha-'+role} for role in self.roles}
        self.opening={**self.heads,'database':'ashlar_g03_test','applicationId':'local-application'}

    def test_six_role_heads_match_with_separate_runtime_identity(self):
        MODULE.assert_closing_snapshot(self.opening,self.heads,self.roles,'ashlar_g03_test','local-application')

    def test_rejects_changed_role_head(self):
        changed=dict(self.heads); changed['manifest']={**changed['manifest'],'version':3}
        with self.assertRaisesRegex(RuntimeError,'six-role Delta heads'):
            MODULE.assert_closing_snapshot(self.opening,changed,self.roles,'ashlar_g03_test','local-application')

    def test_rejects_role_metadata_in_closing_map(self):
        with self.assertRaisesRegex(RuntimeError,'six-role Delta heads'):
            MODULE.assert_closing_snapshot(self.opening,{**self.heads,'database':'ashlar_g03_test'},self.roles,'ashlar_g03_test','local-application')

    def test_rejects_database_or_application_drift(self):
        with self.assertRaisesRegex(RuntimeError,'runtime identity'):
            MODULE.assert_closing_snapshot(self.opening,self.heads,self.roles,'other_database','local-application')
        with self.assertRaisesRegex(RuntimeError,'runtime identity'):
            MODULE.assert_closing_snapshot(self.opening,self.heads,self.roles,'ashlar_g03_test','other-application')


if __name__ == '__main__':
    unittest.main()
