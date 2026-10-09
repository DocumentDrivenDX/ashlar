"""No authenticated client or output write can precede explicit endpoint choice."""
import importlib.util
from pathlib import Path
import sys,tempfile,types,unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
class EndpointTests(unittest.TestCase):
    def test_explicit_endpoint_and_profile_before_client_or_output(self):
        calls=[];sdk=types.ModuleType('databricks.sdk');sdk.WorkspaceClient=lambda **kw:calls.append(kw)
        parent=types.ModuleType('databricks');parent.sdk=sdk
        with patch.dict(sys.modules,{'databricks':parent,'databricks.sdk':sdk}):
            spec=importlib.util.spec_from_file_location('private_test_sql',ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout/persistent_sql.py')
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'run'
            for kwargs in [{},{'warehouse_id':'1'*16},{'profile':'private'},
                           {'warehouse_id':'1'*16,'profile':''},{'warehouse_id':None,'profile':'private'}]:
                with self.subTest(kwargs=kwargs),self.assertRaises((TypeError,ValueError)):module.Client(output,**kwargs)
                self.assertFalse(output.exists());self.assertEqual(calls,[])
            module.Client(output,warehouse_id='1'*16,profile='private')
            self.assertEqual(calls,[{'profile':'private'}]);self.assertTrue(output.is_dir())
