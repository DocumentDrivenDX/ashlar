from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from durable_sql import DurableSQL
from journaled_csv_progress import JournaledCsvProgress,LocalProgressOutcomeUnknown
from ashlar.csv_source import csv_batches
from ashlar.staging import batch_row
from ashlar.source_checkpoint import csv_checkpoint
from ashlar.stored_publisher import _artifact
from ashlar.publisher import PublicationError
from test_durable_effects import API
from test_stored_publisher import Driver

RAW=b'id,entity_version,operation,label\n1,1,create,first\n1,2,replace,second\n'
CONFIG=dict(stream='stream',feed='csv',epoch='one',source_system='example',schema_revision='3',type_id='17',properties={'label':'23'})
class Policy:
    def __init__(self):self.calls=0;self.fail_at=None
    def admit(self,request,descriptor,context):
        self.calls+=1
        if context!='held' or self.calls==self.fail_at:raise PermissionError('Closing source custody refused')
class Resolver:
    def __init__(self):self.held=False;self.closing_failure=False;self.wrong=False
    @contextmanager
    def __call__(self,descriptor,context):
        self.held=True
        try:
            yield None if self.wrong else descriptor
            if self.closing_failure:raise PermissionError('Native pin closure refused')
        finally:self.held=False

class CsvProgressTests(unittest.TestCase):
    def request(self,position,predecessor='initial'):
        batch=list(csv_batches(RAW.splitlines(keepends=True),**{k:v for k,v in CONFIG.items() if k!='stream'}))[position-1]
        row=batch_row(batch);request={'stream':'stream','batch_id':batch.batch_id,'predecessor':predecessor,
            'schema_revisions_json':'{"csv":"3"}','source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest'],'source_checkpoint_json':csv_checkpoint(batch)}
        request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        descriptor=_artifact(Driver().artifact(request),request)[1]
        return request,descriptor
    @contextmanager
    def fixture(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary);source=path/'source.csv';source.write_bytes(RAW)
            journal=DurableSQL(str(path/'journal'),API(),'2439e1f2e37ac563','actor');policy=Policy();resolver=Resolver()
            def progress():return JournaledCsvProgress(journal,source,hashlib.sha256(RAW).hexdigest(),policy,resolver,**CONFIG)
            try:yield journal,source,policy,resolver,progress
            finally:journal.close()
    def test_contiguous_progress_reopen_and_exact_repeat(self):
        with self.fixture() as (journal,source,policy,resolver,factory):
            progress=factory();one,descriptor=self.request(1)
            progress.acknowledge(one,descriptor,'held');self.assertEqual(progress.position(),'1')
            two,second=self.request(2,descriptor.publication_id)
            factory().acknowledge(two,second,'held');self.assertEqual(factory().position(),'2')
            factory().acknowledge(one,descriptor,'held');self.assertEqual(factory().position(),'2')
            self.assertEqual(journal.db.execute('SELECT count(*) FROM csv_consumer_progress').fetchone()[0],2)
            self.assertFalse(resolver.held)
    def test_gap_wrong_predecessor_changed_file_epoch_and_mapping_refuse(self):
        with self.fixture() as (journal,source,policy,resolver,factory):
            progress=factory();two,descriptor=self.request(2)
            with self.assertRaises(PublicationError):progress.acknowledge(two,descriptor,'held')
            one,first=self.request(1);progress.acknowledge(one,first,'held')
            with self.assertRaises(PublicationError):progress.acknowledge(two,descriptor,'held')
            source.write_bytes(RAW.replace(b'second',b'changed'))
            with self.assertRaises(PublicationError):progress.position()
            with self.assertRaises(PublicationError):JournaledCsvProgress(journal,source,hashlib.sha256(source.read_bytes()).hexdigest(),policy,resolver,**CONFIG)
            source.write_bytes(RAW)
            with self.assertRaises(PublicationError):JournaledCsvProgress(journal,source,hashlib.sha256(RAW).hexdigest(),policy,resolver,**dict(CONFIG,schema_revision='4'))
    def test_missing_native_descriptor_and_closing_source_denial_do_not_advance(self):
        with self.fixture() as (journal,source,policy,resolver,factory):
            progress=factory();request,descriptor=self.request(1);resolver.wrong=True
            with self.assertRaises(PublicationError):progress.acknowledge(request,descriptor,'held')
            self.assertEqual(progress.position(),'0');resolver.wrong=False
            policy.calls=0;policy.fail_at=3
            with self.assertRaises(PermissionError):progress.acknowledge(request,descriptor,'held')
            self.assertEqual(progress.position(),'0');self.assertFalse(journal.db.in_transaction)
    def test_postcommit_native_closure_failure_is_explicit_unknown_not_rollback(self):
        with self.fixture() as (journal,source,policy,resolver,factory):
            progress=factory();request,descriptor=self.request(1);resolver.closing_failure=True
            with self.assertRaises(LocalProgressOutcomeUnknown):progress.acknowledge(request,descriptor,'held')
            self.assertEqual(progress.position(),'1')
            resolver.closing_failure=False;progress.acknowledge(request,descriptor,'held')
            self.assertEqual(progress.position(),'1')

    def test_lost_local_commit_receipt_remains_unknown_and_exact_replay_recovers(self):
        with self.fixture() as (journal,source,policy,resolver,factory):
            progress=factory();request,descriptor=self.request(1)
            class LostCommit:
                def __getattr__(self,name):return getattr(journal.db,name)
                def commit(self):
                    journal.db.commit()
                    raise RuntimeError('Lost local commit receipt')
            progress.db=LostCommit()
            with self.assertRaises(LocalProgressOutcomeUnknown):progress.acknowledge(request,descriptor,'held')
            self.assertEqual(factory().position(),'1')
            factory().acknowledge(request,descriptor,'held')
            self.assertEqual(factory().position(),'1')

if __name__=='__main__':unittest.main()
