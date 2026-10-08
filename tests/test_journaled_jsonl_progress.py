from contextlib import contextmanager
import hashlib,json,sqlite3,tempfile,unittest
from pathlib import Path
from dataclasses import replace
import test_journaled_csv_progress as csv_fixture
from journaled_jsonl_progress import JournaledJsonlProgress
from journaled_file_progress import LocalProgressOutcomeUnknown
from ashlar.source import jsonl_batches
from ashlar.source_checkpoint import jsonl_checkpoint,validate_source_checkpoint,CheckpointError
from ashlar.staging import batch_row,StagingError
from ashlar.stored_publisher import _artifact
from ashlar.publisher import PublicationError
from test_stored_publisher import Driver

ROOT=Path(__file__).resolve().parents[1]
RAW=(ROOT/'examples/end-to-end/local-string-source.jsonl').read_bytes()
CONFIG=dict(stream='jsonl-stream',feed='local-jsonl',epoch='example-1')
BATCHES=tuple(jsonl_batches(RAW.splitlines(keepends=True),feed=CONFIG['feed'],epoch=CONFIG['epoch']))
class JsonlProgressTests(unittest.TestCase):
    def request(self,index,predecessor='initial'):
        b=BATCHES[index];row=batch_row(b)
        request=dict(stream=CONFIG['stream'],batch_id=b.batch_id,predecessor=predecessor,schema_revisions_json='{"csv":"3"}',source_batch_json=row['batch_json'],source_batch_digest=row['batch_digest'],source_checkpoint_json=jsonl_checkpoint(b))
        request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        return request,_artifact(Driver().artifact(request),request)[1]
    @contextmanager
    def fixture(self):
        with tempfile.TemporaryDirectory() as d:
            source=Path(d)/'original.jsonl';source.write_bytes(RAW)
            db=sqlite3.connect(str(Path(d)/'journal'));journal=type('Journal',(),{'db':db})()
            policy=csv_fixture.Policy();resolver=csv_fixture.Resolver()
            def progress():return JournaledJsonlProgress(journal,source,hashlib.sha256(RAW).hexdigest(),policy,resolver,**CONFIG)
            try:yield journal,source,policy,resolver,progress
            finally:db.close()
    def test_actual_byte_positions_complete_groups_reopen_and_old_repeat(self):
        with self.fixture() as (j,s,p,r,f):
            one,d=self.request(0);f().acknowledge(one,d,'held')
            self.assertEqual(f().position(),BATCHES[0].cursor_after);self.assertEqual(f().completed_batches(),1)
            two,e=self.request(1,d.publication_id);f().acknowledge(two,e,'held')
            self.assertEqual(f().position(),BATCHES[1].cursor_after);self.assertEqual(f().completed_batches(),2)
            three,g=self.request(2,e.publication_id);f().acknowledge(three,g,'held')
            self.assertEqual(f().position(),str(len(RAW)));self.assertEqual(f().completed_batches(),3)
            f().acknowledge(one,d,'held');self.assertEqual(f().position(),str(len(RAW)))
            self.assertFalse(r.held);self.assertEqual(j.db.execute('select count(*) from jsonl_consumer_progress').fetchone()[0],3)
    def test_gap_changed_original_and_failed_closing_custody_refuse(self):
        with self.fixture() as (j,s,p,r,f):
            one,d=self.request(0);two,e=self.request(1)
            with self.assertRaises(PublicationError):f().acknowledge(two,e,'held')
            r.wrong=True
            with self.assertRaises(PublicationError):f().acknowledge(one,d,'held')
            self.assertEqual(f().position(),'0');r.wrong=False
            r.closing_failure=True
            with self.assertRaises(LocalProgressOutcomeUnknown):f().acknowledge(one,d,'held')
            self.assertEqual(f().position(),BATCHES[0].cursor_after)
            s.write_bytes(RAW+b' ')
            with self.assertRaises(PublicationError):f().position()
    def test_checkpoint_cannot_substitute_group_ordinal_or_other_cursors(self):
        b=BATCHES[0];checkpoint=jsonl_checkpoint(b);value=json.loads(checkpoint)
        self.assertEqual(validate_source_checkpoint(checkpoint,b),value)
        for changes in [{'position':'1'},{'previous':'1'},{'payload_digest':'0'*64},{'profile':'future'}]:
            with self.assertRaises(CheckpointError):validate_source_checkpoint(json.dumps(dict(value,**changes)),b)
        with self.assertRaises((CheckpointError,StagingError)):jsonl_checkpoint(replace(b,cursor_after='00'))
