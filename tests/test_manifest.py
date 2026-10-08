from contextlib import contextmanager
import json
import unittest
from ashlar.native import SQLResult
from ashlar.manifest import DeltaManifestStore,ManifestError,validate_manifest_row

ROW={'publication_id':'p','profile_version':'fixture/1','table_versions_json':'{"c.s.t":0}','source_progress_json':'{"f":{"cursor":"1"}}','schema_revisions_json':'{"f":"1"}','validation_report_json':'{"complete":true}','recorded_at':'1791450000000000'}
class Policy:
    @contextmanager
    def writer(self,*args):yield
    def admit(self,row,context):
        if context!='admitted':raise PermissionError('Missing complete admission')
class Executor:
    def __init__(self):self.row=None;self.calls=[]
    def query(self,sql,p):
        self.calls.append(sql)
        if sql.startswith('DESCRIBE'):return SQLResult([{'id':'uuid'}])
        if sql.startswith('MERGE'):
            candidate=json.loads(p['row'])
            if self.row is not None and candidate!=self.row:raise ManifestError('IMMUTABLE_PUBLICATION_CONFLICT')
            self.row=candidate;return SQLResult([])
        return SQLResult([self.row])
class ManifestTests(unittest.TestCase):
    def test_bound_append_and_exact_readback(self):
        ex=Executor();store=DeltaManifestStore(ex,Policy(),'c.s.manifest','uuid')
        self.assertEqual(store.commit(ROW,context='admitted'),ROW)
        self.assertEqual(len(ex.calls),4)
    def test_replay_preserves_original_and_conflict_refuses(self):
        ex=Executor();store=DeltaManifestStore(ex,Policy(),'c.s.manifest','uuid')
        store.commit(ROW,context='admitted')
        self.assertEqual(store.commit(ROW,context='admitted'),ROW)
        with self.assertRaises(ManifestError):
            store.commit(dict(ROW,recorded_at='1791450000000001'),context='admitted')
        self.assertEqual(ex.row,ROW)
    def test_identity_change_refuses_before_effect(self):
        ex=Executor()
        with self.assertRaises(ManifestError):
            DeltaManifestStore(ex,Policy(),'c.s.manifest','changed').commit(ROW,context='admitted')
        self.assertIsNone(ex.row)
    def test_admission_denial_precedes_manifest_effect(self):
        ex=Executor()
        with self.assertRaises(PermissionError):DeltaManifestStore(ex,Policy(),'c.s.m','uuid').commit(ROW,context='denied')
        self.assertEqual(len(ex.calls),1)
    def test_incomplete_and_malformed_vectors_refuse(self):
        for key,value in [('validation_report_json','{"complete":false}'),('table_versions_json','{"c.s.t":true}'),('recorded_at','01'),('schema_revisions_json','{"f":1}')]:
            with self.assertRaises(ManifestError):validate_manifest_row(dict(ROW,**{key:value}))
if __name__=='__main__':unittest.main()
