"""Ordinary preparation ports only; no DuckDB/native session qualification."""
import hashlib,json,tempfile,types,unittest
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch
from puppy_commerce_fixture import admission
from ashlar_host.puppy_commerce_prepare import PuppyCommercePrepareConfig,prepare_exact_commerce
ROOT=Path(__file__).resolve().parents[1]

class Policy:
    def __init__(self):self.active=False;self.renewals=0;self.closed=False;self.refuse=False
    @contextmanager
    def hold(self,c):
        self.active=True
        try:yield
        finally:self.active=False;self.closed=True
    def admit_exact_commerce(self,p,c):
        assert self.active
        if self.refuse:raise PermissionError('independent source refused')
    def renew_exact_commerce(self,p,c):
        assert self.active;self.renewals+=1

class Database:
    def __init__(self,path,error=None):
        self.path=Path(path);self.path.write_bytes(b'owned inert database')
        self.rows={};self.selected=None;self.commands=[];self.closed=False;self.error=error
    def execute(self,sql):
        self.commands.append(sql)
        if sql.startswith('SELECT '):self.selected=sql.rsplit('.',1)[1]
        return self
    def executemany(self,sql,rows):
        if self.error is not None:raise self.error
        self.rows[sql.split()[2].split('.')[1]]=rows
    def fetchall(self):return [r[:-2]for r in self.rows[self.selected]]
    def close(self):self.closed=True

class PreparationTests(unittest.TestCase):
    def config(self,output):
        root=ROOT/'docs/helix/04-build/evidence/commerce-graph-export-20261009'
        release=(root/'release.graph.json').read_bytes();custody=(root/'custody.json').read_bytes()
        return PuppyCommercePrepareConfig(release,custody,hashlib.sha256(release).hexdigest(),hashlib.sha256(custody).hexdigest(),(ROOT/'examples/domain-packs/commerce/upstream/pack.json').read_bytes(),output)
    def test_complete_owned_readback_and_private_output(self):
        with tempfile.TemporaryDirectory()as d:
            output=Path(d).resolve()/'prepared';policy=Policy();opened=[]
            def connect(path):opened.append(Database(path));return opened[-1]
            with patch.dict('sys.modules',duckdb=types.SimpleNamespace(connect=connect)):
                result=prepare_exact_commerce(self.config(output),policy,None,source_admission=admission(self))
            old=json.loads((ROOT/'docs/helix/04-build/evidence/commerce-exact-puppygraph-20261009/receipt.json').read_bytes())
            for k in ('nodes','edges','numeric_input_guards','all_unfiltered_intermediate_guards','expected'):self.assertEqual(result[k],old[k])
            self.assertIn('COMMIT',opened[0].commands);self.assertTrue(opened[0].closed);self.assertTrue(policy.closed)
            self.assertEqual(output.stat().st_mode&0o777,0o700)
            for name in ('commerce-exact.duckdb','model.json','receipt.json'):self.assertEqual((output/name).stat().st_mode&0o777,0o444)
            model=json.loads((output/'model.json').read_bytes());self.assertTrue(all(a['type']=='LONG'for n in model['node']for a in n['attribute']if a['name']in('quantity_long','amount_coefficient','unit_price_coefficient')))
    def test_refusal_and_duplicate_before_sdk_connection(self):
        with tempfile.TemporaryDirectory()as d:
            for duplicate in (False,True):
                output=Path(d).resolve()/('duplicate'if duplicate else'refused');policy=Policy();policy.refuse=not duplicate
                if duplicate:output.mkdir()
                with patch.dict('sys.modules',duckdb=types.SimpleNamespace(connect=lambda p:(_ for _ in ()).throw(AssertionError('SDK acquired')))):
                    with self.assertRaises((PermissionError,ValueError)):prepare_exact_commerce(self.config(output),policy,None,source_admission=admission(self))
                self.assertTrue(policy.closed)
    def test_body_cancellation_rolls_back_closes_and_renews(self):
        with tempfile.TemporaryDirectory()as d:
            cancel=KeyboardInterrupt();policy=Policy();opened=[]
            def connect(path):opened.append(Database(path,cancel));return opened[-1]
            with patch.dict('sys.modules',duckdb=types.SimpleNamespace(connect=connect)):
                with self.assertRaises(KeyboardInterrupt)as caught:prepare_exact_commerce(self.config(Path(d).resolve()/'out'),policy,None,source_admission=admission(self))
            self.assertIs(caught.exception,cancel);self.assertIn('ROLLBACK',opened[0].commands);self.assertTrue(opened[0].closed);self.assertGreaterEqual(policy.renewals,2);self.assertTrue(policy.closed)

    def test_commit_failure_withholds_receipt_without_speculative_rollback(self):
        with tempfile.TemporaryDirectory()as d:
            output=Path(d).resolve()/'out';opened=[];policy=Policy()
            class CommitFailure(Database):
                def execute(self,sql):
                    super().execute(sql)
                    if sql=='COMMIT':raise OSError('commit outcome unknown')
                    return self
            def connect(path):opened.append(CommitFailure(path));return opened[-1]
            with patch.dict('sys.modules',duckdb=types.SimpleNamespace(connect=connect)):
                with self.assertRaises(OSError):prepare_exact_commerce(self.config(output),policy,None,source_admission=admission(self))
            self.assertNotIn('ROLLBACK',opened[0].commands);self.assertTrue(opened[0].closed);self.assertTrue(policy.closed);self.assertFalse((output/'receipt.json').exists())

    def test_missing_sdk_keeps_output_absent_and_closes_authority(self):
        with tempfile.TemporaryDirectory()as d:
            output=Path(d).resolve()/'out';policy=Policy()
            with patch.dict('sys.modules',duckdb=None):
                with self.assertRaises(ImportError):prepare_exact_commerce(self.config(output),policy,None,source_admission=admission(self))
            self.assertFalse(output.exists());self.assertTrue(policy.closed);self.assertGreater(policy.renewals,0)

    def test_changed_genuine_source_before_commit_withholds_public_artifacts(self):
        with tempfile.TemporaryDirectory()as d:
            output=Path(d).resolve()/'out';policy=Policy();source=admission(self);opened=[]
            class SourceDrift(Database):
                def fetchall(self):
                    source.originals[0][0].write_bytes(b'changed original source')
                    return super().fetchall()
            def connect(path):opened.append(SourceDrift(path));return opened[-1]
            with patch.dict('sys.modules',duckdb=types.SimpleNamespace(connect=connect)):
                with self.assertRaises(PermissionError):prepare_exact_commerce(self.config(output),policy,None,source_admission=source)
            self.assertNotIn('COMMIT',opened[0].commands);self.assertIn('ROLLBACK',opened[0].commands);self.assertTrue(opened[0].closed);self.assertTrue(policy.closed);self.assertFalse((output/'receipt.json').exists())

if __name__=='__main__':unittest.main()
