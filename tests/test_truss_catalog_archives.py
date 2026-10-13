"""Archive correspondence only; no native or current-head substitute."""
import base64,hashlib,json,sys,tempfile,types,unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch
from ashlar_host.truss_catalog_reader import parse_archived_acceptance_files,read_historical_catalog_views

class Tests(unittest.TestCase):
    def test_original_acceptance_archive_matches_source_report_and_refuses_changed_report(self):
        root=Path('docs/helix/04-build/evidence/truss-schema-acceptance-20261012');report=(root/'report.json').read_bytes();catalog=(root/'catalog.json').read_bytes();view=json.loads(catalog);reads=[]
        @dataclass
        class Pin:identity:str;version:str;sha256:str
        @dataclass
        class Request:selection:object;layout_profile:object;view_profile:object
        @dataclass
        class Ok:value:object
        @dataclass
        class Historical:catalog_revision:str
        @dataclass
        class Available:view:object
        @dataclass
        class Unavailable:reason:str='context'
        class Assembly:
            def catalog_view_request(self):return Request('current-head-2',Pin(**view['layoutProfile']),Pin(**view['viewProfile']))
            def catalog_view_in_transaction(self,tx,request):reads.append(request.selection.catalog_revision);return Ok(Unavailable())
            def dispose(self):pass
        class Host:
            def adopt_transaction(self):return Ok('tx')
            def detach(self):return True
        class Connection:
            autocommit=False;info=types.SimpleNamespace(transaction_status=0)
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def execute(self,sql):pass
            def rollback(self):pass
        sdk=types.ModuleType('truss');sdk.Ok=Ok;sdk.HistoricalCatalogSelection=Historical;sdk.CatalogViewAvailable=Available;sdk.CatalogViewUnavailable=Unavailable;sdk.CatalogRuntimeConfiguration=lambda:None;sdk.adopt_psycopg_host=lambda connection:Host();sdk.create_reference_assembly=lambda config,host:Ok(Assembly())
        batch=types.SimpleNamespace(profile='truss-native-feed/0.1',begin=json.dumps({'prior':{'installationId':json.loads(report)['installationId']},'catalogPrerequisites':[{'revision':'1','reportBytesBase64':base64.b64encode(report).decode()}]}).encode())
        with tempfile.TemporaryDirectory()as temporary:
            path=Path(temporary).resolve()/'acceptance.json';archive={'report_original_base64':base64.b64encode(report).decode(),'catalog_original_base64':base64.b64encode(catalog).decode()};path.write_text(json.dumps(archive))
            def selected():return parse_archived_acceptance_files([{'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}])
            with patch.dict(sys.modules,{'truss':sdk}),patch('ashlar_host.truss_catalog_reader.batch_row'):
                self.assertEqual(read_historical_catalog_views(Connection,batch,selected()),{'1':catalog});self.assertEqual(reads,[])
                archive['report_original_base64']=base64.b64encode(report+b' ').decode();path.write_text(json.dumps(archive))
                with self.assertRaises(ValueError):read_historical_catalog_views(Connection,batch,selected())
                self.assertEqual(reads,['1'])
