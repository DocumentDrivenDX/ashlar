"""Effect-free tests for local Delta detail URI and UUID identity checks."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('prepare.py')
SPEC = importlib.util.spec_from_file_location('g03_native_prepare_location', SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

class DeltaLocationIdentityTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix='delta location ')).resolve()

    def test_file_uri_forms_and_percent_encoding(self):
        encoded = str(self.root).replace(' ', '%20')
        self.assertEqual(MODULE.normalize_local_delta_location('file:' + encoded), self.root)
        self.assertEqual(MODULE.normalize_local_delta_location('file://' + encoded), self.root)
        self.assertEqual(MODULE.normalize_local_delta_location(str(self.root)), self.root)

    def test_uuid_is_checked_independently_of_location(self):
        with self.assertRaises(RuntimeError) as caught:
            MODULE.assert_delta_alias_identity({'id':'wrong','location':'file:'+str(self.root)},'expected',self.root)
        self.assertEqual(caught.exception.diagnostics['actualUuid'],'wrong')
        self.assertEqual(caught.exception.diagnostics['normalizedLocation'],str(self.root))
        self.assertEqual(caught.exception.diagnostics['expectedUuid'],'expected')

    def test_location_is_checked_independently_of_uuid(self):
        with self.assertRaises(RuntimeError) as caught:
            MODULE.assert_delta_alias_identity({'id':'expected','location':'file:/foreign/path'},'expected',self.root)
        self.assertEqual(caught.exception.diagnostics['actualUuid'],'expected')
        self.assertEqual(caught.exception.diagnostics['rawLocation'],'file:/foreign/path')
        self.assertNotEqual(caught.exception.diagnostics['normalizedLocation'],str(self.root))

    def test_remote_or_query_bearing_locations_are_rejected(self):
        for value in ('s3://bucket/path','file://remote/path','file:/tmp/path?x=1'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                MODULE.normalize_local_delta_location(value)

if __name__ == '__main__':
    unittest.main()
