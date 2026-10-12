import base64,json,sys,tempfile,types,unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch
from ashlar_host.truss_schema_acceptance import run_accept_truss_schema_command

class Tests(unittest.TestCase):
    def test_readback_selects_accepted_historical_revision_after_head_advanced(self):
        calls=[]
        @dataclass
        class Ok:value:object
        @dataclass
        class Registered:
            revision:int=1
            registration_id:str='actual-registration'
            replayed:bool=False
        @dataclass
        class Unchanged:pass
        @dataclass
        class Historical:
            catalog_revision:str
            def __post_init__(self):
                if type(self.catalog_revision)is not str:raise ValueError('Public SDK requires revision text')
        @dataclass
        class Request:selection:object
        @dataclass
        class Available:view:object
        original_catalog=b'{"catalogRevision":"1","original":"accepted catalog"}'
        original_report=b'{"revision":"1","original":"accepted report"}'
        class Assembly:
            def status_in_transaction(self,tx):return Ok(types.SimpleNamespace(installed=True))
            def prepare_catalog(self,documents,**kwargs):return documents
            def accept_in_transaction(self,tx,prepared,**kwargs):calls.append('accepted-1');return Ok(Registered())
            def report_in_transaction(self,tx,revision):calls.append(('report',revision));return Ok(types.SimpleNamespace(original_bytes=original_report))
            def catalog_view_request(self,**kwargs):return Request(types.SimpleNamespace(kind='current',current_head=2))
            def catalog_view_in_transaction(self,tx,request):
                calls.append(('catalog',request.selection.catalog_revision));return Ok(Available(types.SimpleNamespace(original_bytes=original_catalog)))
            def dispose(self):calls.append('dispose')
        class Host:
            def adopt_transaction(self):return Ok('tx')
            def detach(self):calls.append('detach');return True
        class Connection:
            def __enter__(self):return self
            def __exit__(self,*args):calls.append('connection-close')
            def execute(self,sql):pass
            def commit(self):calls.append('commit')
        truss=types.ModuleType('truss');truss.Ok=Ok;truss.CatalogRegistration=Registered;truss.CatalogUnchanged=Unchanged;truss.CatalogViewAvailable=Available;truss.HistoricalCatalogSelection=Historical;truss.CatalogRuntimeConfiguration=lambda:None;truss.CatalogDocument=lambda *args:args;truss.adopt_psycopg_host=lambda connection:Host();truss.create_reference_assembly=lambda config,host:Ok(Assembly())
        psycopg=types.ModuleType('psycopg');psycopg.connect=lambda *args,**kwargs:Connection()
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);bun=root/'bun';bun.write_text('selected installed executable');document=root/'original.umf.json';document.write_text('{"id":"original"}');origin=root/'origin.json';origin.write_text('{}');configuration=root/'configuration.json';output=root/'result.json'
            configuration.write_text(json.dumps({'connection_uri':'private ordinary connection','bun':str(bun),'documents':[{'path':str(document),'document_id':'original','document_revision':'r1'}],'request_key':'actual-request','origin_path':str(origin)}))
            with patch.dict(sys.modules,{'truss':truss,'psycopg':psycopg}):result=run_accept_truss_schema_command(configuration,output)
            self.assertEqual(result['revision'],1);self.assertIn(('catalog','1'),calls)
            self.assertEqual(base64.b64decode(result['catalog_original_base64']),original_catalog)
            self.assertEqual(base64.b64decode(result['report_original_base64']),original_report)
            self.assertEqual(json.loads(output.read_bytes()),result)
            self.assertEqual(calls[-4:],['dispose','detach','commit','connection-close'])
