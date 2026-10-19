# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original version preflight; public SDK owns actual mutation semantics."""
import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host import truss_record_update,truss_record_delete
import test_truss_record_update as updates
import test_truss_record_delete as deletes

ROOT=Path(__file__).resolve().parents[1]

class Tests(unittest.TestCase):
    def test_both_declared_versions_preserve_original_source_bytes(self):
        for host,fixture in ((truss_record_update,updates),(truss_record_delete,deletes)):
            for name in ('typed-query-record.umf.json','core08-asset.umf.json'):
                with self.subTest(host=host.__name__,model=name),tempfile.TemporaryDirectory()as directory:
                    root=Path(directory).resolve();configuration,config=fixture.Tests().inputs(root)
                    original=(ROOT/'examples/truss-native'/name).read_bytes()
                    document=json.loads(original);Path(config['document_path']).write_bytes(original)
                    config.update(document_id=document['id'],module_id=document['modules'][0]['id'])
                    configuration.write_text(json.dumps(config))
                    selected=host.preflight(configuration,root/'report.json')
                    self.assertEqual(selected[3],original)
                    self.assertFalse((root/'report.json').exists())

    def test_future_version_refuses_before_sdk_import_or_report(self):
        actual_import=__import__
        def import_guard(name,*args,**kwargs):
            if name in ('psycopg','truss'):raise AssertionError('Native SDK effects forbidden')
            return actual_import(name,*args,**kwargs)
        for host,fixture,run in ((truss_record_update,updates,truss_record_update.run_update_truss_record_command),
                (truss_record_delete,deletes,truss_record_delete.run_delete_truss_record_command)):
            with tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();configuration,config=fixture.Tests().inputs(root)
                Path(config['document_path']).write_text('{"id":"doc","umf":"0.9.0"}')
                with patch('builtins.__import__',side_effect=import_guard):
                    with self.assertRaises(ValueError):run(configuration,root/'report.json')
                self.assertFalse((root/'report.json').exists())

if __name__=='__main__':unittest.main()
