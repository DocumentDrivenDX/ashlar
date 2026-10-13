"""Declared source-version routing, without SDK acceptance authority."""
import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.truss_schema_evolution import require_matching_record_versions,parse_truss_record_evolution_configuration,run_evolve_truss_record_command

class Tests(unittest.TestCase):
    def test_exact_bytes_and_matching_version_selection(self):
        for version in ('0.7.0','0.8.0'):
            with self.subTest(version=version),tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();source=(' { "umf": "'+version+'", "id": "asset", "extensions": {"opaque": [1,"retained"]} }\n').encode()
                model=root/'model.json';model.write_bytes(source);bun=root/'bun';bun.write_text('inert');origin=root/'origin.json';origin.write_text('{}')
                config={'connection_uri':'inert','bun':str(bun),'document_path':str(model),'document_revision':'r2','origin_path':str(origin),
                    'accept_request_key':'accept','update_request_key':'update','object_id':1,'document_id':'asset','module_id':'assets','record_id':'Asset','field_name':'note','field_value':'retained'}
                self.assertEqual(parse_truss_record_evolution_configuration(config,root/'report.json')[0],source)
                self.assertEqual(require_matching_record_versions(source,source),version)
                old=json.dumps({'id':'asset','umf':'0.8.0'if version=='0.7.0'else'0.7.0'}).encode()
                with self.assertRaises(ValueError):require_matching_record_versions(source,old)
                self.assertEqual(model.read_bytes(),source)

    def test_future_version_refuses_before_sdk_import(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();model=root/'model';model.write_text('{"id":"asset","umf":"0.9.0"}')
            config={'connection_uri':'inert','bun':str(root/'bun'),'document_path':str(model),'document_revision':'r2','origin_path':str(root/'origin'),
                'accept_request_key':'accept','update_request_key':'update','object_id':1,'document_id':'asset','module_id':'assets','record_id':'Asset','field_name':'note','field_value':'retained'}
            path=root/'configuration';path.write_text(json.dumps(config));original_import=__import__
            def guarded(name,*args,**kwargs):
                if name in ('psycopg','truss'):raise AssertionError('No SDK effects permitted')
                return original_import(name,*args,**kwargs)
            with patch('builtins.__import__',side_effect=guarded):
                with self.assertRaises(ValueError):run_evolve_truss_record_command(path,root/'report')
            self.assertFalse((root/'report').exists())

if __name__=='__main__':unittest.main()
