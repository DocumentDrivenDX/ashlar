# SPDX-License-Identifier: MIT OR Apache-2.0
import json,sys,types,unittest
from dataclasses import dataclass
from unittest.mock import patch
from ashlar_host.truss_catalog_reader import read_historical_catalog_views

class Tests(unittest.TestCase):
    def test_048_and_049_current_reads_use_pinned_runtime_and_two_second_transaction(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from test_managed_current_core import _current_admission
        from test_truss_current_catalog import receipt_inputs
        from ashlar_host.truss_catalog_reader import read_current_catalog_evidence
        from ashlar_host.truss_current_catalog import (CURRENT_048_EVIDENCE_PROFILE,
            CURRENT_TRUSS_048_WHEEL_SHA256, CURRENT_TRUSS_048_RUNTIME_MANIFEST_SHA256,
            CURRENT_049_EVIDENCE_PROFILE, CURRENT_TRUSS_049_WHEEL_SHA256,
            CURRENT_TRUSS_049_RUNTIME_MANIFEST_SHA256, CURRENT_050_EVIDENCE_PROFILE,
            CURRENT_TRUSS_050_WHEEL_SHA256, CURRENT_TRUSS_050_RUNTIME_MANIFEST_SHA256,
            CURRENT_051_EVIDENCE_PROFILE, CURRENT_TRUSS_051_WHEEL_SHA256,
            CURRENT_TRUSS_051_RUNTIME_MANIFEST_SHA256)

        report_raw, view_raw, sources, source = receipt_inputs()
        calls = []
        @dataclass
        class Ok:
            value: object
        @dataclass
        class Available:
            view: object
        class Assembly:
            def catalog_view_request(self, **kwargs):
                calls.append(('request', kwargs)); return object()
            def accepted_document_in_transaction(self, tx, *, document_id, maximum_bytes):
                calls.append(('source', tx, document_id, maximum_bytes))
                return Ok(source)
            def catalog_view_in_transaction(self, tx, request):
                calls.append(('view', tx, request)); return Ok(Available(SimpleNamespace(original_bytes=view_raw)))
            def dispose(self): calls.append('dispose')
        class Host:
            def adopt_transaction(self, **kwargs): calls.append(('transaction', kwargs)); return Ok('tx')
            def detach(self): calls.append('detach'); return True
        class Connection:
            autocommit = False
            info = SimpleNamespace(transaction_status=0)
            def __enter__(self): return self
            def __exit__(self, *args): calls.append('connection-close')
            def execute(self, sql): calls.append(sql)
            def rollback(self): calls.append('rollback')

        batch = _current_admission().batch
        sdk = types.ModuleType('truss')
        sdk.Ok = Ok; sdk.CatalogViewAvailable = Available
        sdk.CatalogViewUnavailable = type('Unavailable', (), {})
        sdk.CatalogRuntimeConfiguration = lambda: None
        sdk.adopt_psycopg_host = lambda connection: Host()
        sdk.create_reference_assembly = lambda config, host: Ok(Assembly())
        for version, profile, wheel, manifest in (
                ('0.0.48', CURRENT_048_EVIDENCE_PROFILE,
                 CURRENT_TRUSS_048_WHEEL_SHA256, CURRENT_TRUSS_048_RUNTIME_MANIFEST_SHA256),
                ('0.0.49', CURRENT_049_EVIDENCE_PROFILE,
                 CURRENT_TRUSS_049_WHEEL_SHA256, CURRENT_TRUSS_049_RUNTIME_MANIFEST_SHA256),
                ('0.0.50', CURRENT_050_EVIDENCE_PROFILE,
                 CURRENT_TRUSS_050_WHEEL_SHA256, CURRENT_TRUSS_050_RUNTIME_MANIFEST_SHA256),
                ('0.0.51', CURRENT_051_EVIDENCE_PROFILE,
                 CURRENT_TRUSS_051_WHEEL_SHA256, CURRENT_TRUSS_051_RUNTIME_MANIFEST_SHA256)):
            calls.clear()
            with patch.dict(sys.modules, {'truss': sdk}), \
                 patch('ashlar_host.truss_catalog_reader.installed_truss_version', return_value=version), \
                 patch('ashlar_host.truss_catalog_reader.installed_current_truss_profile',
                       return_value=profile):
                evidence = read_current_catalog_evidence(Connection, batch)
            self.assertEqual(evidence.truss_version, version)
            self.assertEqual(evidence.runtime_wheel_sha256, wheel)
            self.assertEqual(evidence.runtime_manifest_sha256, manifest)
            self.assertIn("SET LOCAL statement_timeout = '2000ms'", calls)
            self.assertIn(('transaction', {'access_mode': 'read_write'}), calls)
            source_transactions = [entry[1] for entry in calls if type(entry) is tuple and entry[0] == 'source']
            view_transactions = [entry[1] for entry in calls if type(entry) is tuple and entry[0] == 'view']
            self.assertEqual(source_transactions, ['tx'])
            self.assertEqual(view_transactions, source_transactions)
            self.assertEqual(calls[-4:], ['dispose', 'detach', 'rollback', 'connection-close'])

    def test_selected_historical_revisions_preserve_bytes_and_rollback_catalog_lock_transaction(self):
        calls=[]
        @dataclass
        class Ok:value:object
        @dataclass
        class Historical:catalog_revision:str
        @dataclass
        class Request:selection:object
        @dataclass
        class Available:view:object
        @dataclass
        class Unavailable:reason:str
        originals={'1':b'{"catalogRevision":"1","unknown":"original"}','2':b'{"catalogRevision":"2"}'}
        class Assembly:
            def catalog_view_request(self):return Request(None)
            def catalog_view_in_transaction(self,tx,request):
                calls.append(('read',request.selection.catalog_revision));return Ok(Available(types.SimpleNamespace(original_bytes=originals[request.selection.catalog_revision])))
            def dispose(self):calls.append('dispose')
        class Host:
            def adopt_transaction(self,**kwargs):calls.append(kwargs);return Ok('tx')
            def detach(self):calls.append('detach');return True
        class Connection:
            autocommit=False;info=types.SimpleNamespace(transaction_status=0)
            def __enter__(self):return self
            def __exit__(self,*args):calls.append('connection-close')
            def execute(self,sql):calls.append(sql)
            def rollback(self):calls.append('rollback')
        sdk=types.ModuleType('truss');sdk.Ok=Ok;sdk.HistoricalCatalogSelection=Historical;sdk.CatalogViewAvailable=Available;sdk.CatalogViewUnavailable=Unavailable;sdk.CatalogRuntimeConfiguration=lambda:None;sdk.adopt_psycopg_host=lambda connection:Host();sdk.create_reference_assembly=lambda config,host:Ok(Assembly())
        batch=types.SimpleNamespace(profile='truss-native-feed/0.1',begin=json.dumps({'catalogPrerequisites':[{'revision':'0','reportBytesBase64':None},{'revision':'1','reportBytesBase64':'original'},{'revision':'2','reportBytesBase64':'original'}]}).encode())
        with patch.dict(sys.modules,{'truss':sdk}),patch('ashlar_host.truss_catalog_reader.batch_row'):
            actual=read_historical_catalog_views(Connection,batch)
        self.assertEqual(actual,originals);self.assertIn({'access_mode':'read_write'},calls);self.assertIn('SET TRANSACTION ISOLATION LEVEL READ COMMITTED',calls);self.assertNotIn('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY',calls)
        self.assertEqual([entry for entry in calls if type(entry)is tuple],[('read','1'),('read','2')])
        self.assertEqual(calls[-4:],['dispose','detach','rollback','connection-close'])

        with patch.dict(sys.modules,{'truss':sdk}),patch('ashlar_host.truss_catalog_reader.batch_row'),patch.object(Assembly,'catalog_view_in_transaction',return_value=Ok(Unavailable('context'))):
            with self.assertRaisesRegex(ValueError,'Public historical catalog view unavailable: context'):
                read_historical_catalog_views(Connection,batch)
        self.assertEqual(calls[-3:],['dispose','detach','connection-close'])
