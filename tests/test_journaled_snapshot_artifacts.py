import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from durable_sql import DurableSQL
from journaled_snapshot_artifacts import JournaledSnapshotArtifacts
from ashlar.publisher import PublicationError
from ashlar.effect_validation import EffectValidationError
from test_effect_validation import Executor,COLS,ROW
from test_durable_effects import API
from test_stored_publisher import Driver
from ashlar.csv_source import csv_batches
from ashlar.staging import batch_row
from ashlar.source_checkpoint import csv_checkpoint

class Policy:
    def __init__(self):self.denied=False;self.calls=0
    def admit(self,request,effects,targets,context):
        self.calls+=1
        if self.denied or context!='held':raise PermissionError('Denied')

class SnapshotArtifactTests(unittest.TestCase):
    def request(self):
        batch=next(csv_batches([b'id,entity_version,operation,label\n',b'1,1,create,label\n'],feed='csv',epoch='one',source_system='example',schema_revision='3',type_id='17',properties={'label':'23'}))
        row=batch_row(batch)
        request={'stream':'stream','batch_id':batch.batch_id,'predecessor':'prior','schema_revisions_json':'{"csv":"3"}',
            'source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest'],'source_checkpoint_json':csv_checkpoint(batch)}
        request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        return request
    def fixture(self,path,executor,policy,manifest=None):
        journal=DurableSQL(str(path),API(),'2439e1f2e37ac563','actor')
        def targets(request,effects,context):return {'c.s.object_current':{'uuid':'uuid','version':7,'columns':COLS,'rows':[ROW]}}
        def row(request,effects,witnesses,context):return json.loads(Driver().artifact(request))['manifest']
        return journal,JournaledSnapshotArtifacts(journal,executor,policy,targets,manifest or row,namespace='original')
    def test_capture_then_reopen_preserves_bytes_without_snapshot_queries(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'journal';executor=Executor();policy=Policy();request=self.request()
            effects={'intent_digest':request['request_digest'],'plan_digest':'a'*64,'responses':[]}
            journal,artifacts=self.fixture(path,executor,policy)
            original=artifacts.capture(request,effects,'held');self.assertEqual(len(executor.calls),4)
            journal.close()
            journal,artifacts=self.fixture(path,executor,policy)
            self.assertEqual(artifacts.recover(request,effects,'held'),original)
            self.assertEqual(artifacts.capture(request,effects,'held'),original)
            self.assertEqual(len(executor.calls),4)
            policy.denied=True
            with self.assertRaises(PermissionError):artifacts.recover(request,effects,'held')
            journal.close()
    def test_failed_capture_and_missing_custody_never_observe_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            executor=Executor([]);request=self.request();effects={'intent_digest':request['request_digest']}
            journal,artifacts=self.fixture(Path(temporary)/'journal',executor,Policy())
            with self.assertRaises(PublicationError):artifacts.recover(request,effects,'held')
            self.assertEqual(executor.calls,[])
            with self.assertRaises(EffectValidationError):artifacts.capture(request,effects,'held')
            before=list(executor.calls);executor.rows=[ROW]
            for method in (artifacts.capture,artifacts.recover):
                with self.assertRaises(PublicationError):method(request,effects,'held')
            self.assertEqual(executor.calls,before);journal.close()
    def test_vector_mismatch_and_corrupt_artifact_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            request=self.request();effects={'intent_digest':request['request_digest']};executor=Executor()
            def wrong(request,effects,witnesses,context):
                row=json.loads(Driver().artifact(request))['manifest'];row['table_versions_json']='{"c.s.object_current":8}';return row
            journal,artifacts=self.fixture(Path(temporary)/'wrong',executor,Policy(),wrong)
            with self.assertRaises(PublicationError):artifacts.capture(request,effects,'held')
            journal.close()
            journal,artifacts=self.fixture(Path(temporary)/'valid',Executor(),Policy())
            artifacts.capture(request,effects,'held')
            targets=journal.db.execute('SELECT targets_json FROM snapshot_artifact').fetchone()[0]
            with journal.db:journal.db.execute("UPDATE snapshot_artifact SET targets_json='{}'")
            with self.assertRaises(PublicationError):artifacts.recover(request,effects,'held')
            with journal.db:
                journal.db.execute('UPDATE snapshot_artifact SET targets_json=?,artifact_digest=?',(targets,'0'*64))
            with self.assertRaises(PublicationError):artifacts.recover(request,effects,'held')
            journal.close()

if __name__=='__main__':unittest.main()
