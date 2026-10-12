import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.finite_export_cli import parse_finite_export_configuration,EXPORT_PROFILE

class Tests(unittest.TestCase):
    def test_closed_export_resources_parse_without_weft_settings_or_effects(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve()
            source={'profile':EXPORT_PROFILE,'pack':'archaeology','publication':str(root/'publication'),'jars':str(root/'jars'),'model':str(root/'model.json'),'graph':str(root/'graph.json'),'pack_file':str(root/'pack.json'),'producer':{'source':str(root/'umf'),'bun':str(root/'bun'),'git':str(root/'git'),'timeout_seconds':5,'maximum_output_bytes':1048576,'maximum_receipt_bytes':33554432},'operation_capacity':None}
            original_import=__import__
            def guarded(name,*args,**kwargs):
                if name.startswith(('weft','pyspark','truss')):raise AssertionError('Resource parsing must not load optional SDKs')
                return original_import(name,*args,**kwargs)
            with patch('builtins.__import__',side_effect=guarded):
                selected=parse_finite_export_configuration(json.dumps(source).encode(),root/'export')
                self.assertIsNone(selected.query);self.assertEqual(selected.native.output,root/'publication');self.assertEqual(selected.native.producer.definition.name,'archaeology')
                with self.assertRaises(ValueError):parse_finite_export_configuration(json.dumps({**source,'query':{}}).encode(),root/'export')
                duplicated=json.dumps(source)[:-1]+',"pack":"ecology"}'
                with self.assertRaises(ValueError):parse_finite_export_configuration(duplicated.encode(),root/'export')
            self.assertEqual(list(root.iterdir()),[])
