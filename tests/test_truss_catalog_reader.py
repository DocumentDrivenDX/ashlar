import json,sys,types,unittest
from dataclasses import dataclass
from unittest.mock import patch
from ashlar_host.truss_catalog_reader import read_historical_catalog_views

class Tests(unittest.TestCase):
    def test_selected_historical_revisions_preserve_bytes_and_close_readonly_owners(self):
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
        self.assertEqual(actual,originals);self.assertIn({'access_mode':'read_only'},calls)
        self.assertEqual([entry for entry in calls if type(entry)is tuple],[('read','1'),('read','2')])
        self.assertEqual(calls[-4:],['dispose','detach','rollback','connection-close'])

        with patch.dict(sys.modules,{'truss':sdk}),patch('ashlar_host.truss_catalog_reader.batch_row'),patch.object(Assembly,'catalog_view_in_transaction',return_value=Ok(Unavailable('context'))):
            with self.assertRaisesRegex(ValueError,'Public historical catalog view unavailable: context'):
                read_historical_catalog_views(Connection,batch)
        self.assertEqual(calls[-3:],['dispose','detach','connection-close'])
