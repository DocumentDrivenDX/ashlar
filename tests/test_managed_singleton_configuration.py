# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed configuration controls; no credential reads, producer or services."""
import copy,json,tempfile,unittest
from pathlib import Path
from ashlar_host.managed_singleton_configuration import parse_managed_singleton

class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        root=Path(self.temp.name).resolve();p=lambda name:str(root/name)
        pin=lambda name:{'path':p(name),'sha256':'a'*64}
        self.value={'profile':'ashlar-managed-singleton/0.1',
            'selection':dict(installation=pin('installation'),intake_proof=pin('intake'),source_configuration=pin('source.json'),journal=p('journal'),publication_id='original-publication',completed_ordinal=2,maximum_journal_bytes=1048576,maximum_rows=1000),
            'sql':dict(host='https://adb-123.1.azuredatabricks.net',profile='explicit',config_file=p('credentials'),cli=p('databricks'),warehouse_id='a'*16,warehouse_name='dedicated',warehouse_creator='owner',actor_id='original-id',actor_name='owner',statement_seconds=30,http_seconds=5,maximum_rows=1000,maximum_result_bytes=1048576,maximum_statements=128),
            'pins':dict(profile='ashlar-managed-pin-reader/0.1',host='127.0.0.1',hostaddr='127.0.0.1',port=25440,expected_server_address='172.18.0.2',expected_server_port=5432,dbname='pins',user='ordinary_reader',sslmode='disable',passfile=p('private-passfile'),connect_timeout_seconds=5,statement_timeout_ms=5000,lock_timeout_ms=1000),
            'producer':dict(source=p('umf'),bun=p('bun'),git=p('git'),timeout_seconds=30,maximum_output_bytes=1048576,maximum_receipt_bytes=1048576),
            'selector':dict(source_system='original',type_id=17,entity_id=1),
            'output':p('fresh-output'),'bounds':dict(whole_seconds=300,stream_bytes=1048576,receipt_bytes=1048576)}
    def parse(self,value=None):return parse_managed_singleton(json.dumps(self.value if value is None else value).encode())
    def test_complete_explicit_selection_without_service_or_file_access(self):
        config=self.parse();self.assertEqual(config.selection.completed_ordinal,2)
        self.assertEqual(config.selector.source,'original');self.assertEqual(config.cleanup_seconds,50)
        self.assertFalse(config.output.exists());self.assertFalse(config.pins.passfile.exists())
    def test_unknown_duplicate_and_missing_operator_settings_refuse(self):
        for path in (('extra',),('sql','maximum_statements'),('selection','completed_ordinal'),('producer','git')):
            value=copy.deepcopy(self.value)
            if len(path)==1:value[path[0]]=True
            else:value[path[0]].pop(path[1])
            with self.subTest(path=path),self.assertRaises(ValueError):self.parse(value)
        raw=json.dumps(self.value).encode()
        with self.assertRaises(ValueError):parse_managed_singleton(b'{"profile":"other",'+raw[1:])
    def test_insufficient_cleanup_capacity_and_protected_output_refuse(self):
        for kind in ('deadline','statement-cap','output','bool'):
            value=copy.deepcopy(self.value)
            if kind=='deadline':value['bounds']['whole_seconds']=60
            elif kind=='statement-cap':value['sql']['maximum_statements']=64
            elif kind=='output':value['output']=value['producer']['source']
            else:value['selector']['type_id']=True
            with self.subTest(kind=kind),self.assertRaises(ValueError):self.parse(value)
