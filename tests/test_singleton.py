from contextlib import contextmanager
import hashlib,json
import unittest
from ashlar.singleton import read_singleton
from ashlar.pins import PinVector
from ashlar.native import SQLResult
from ashlar.publication import ResolutionError
from test_publication import FakeBackend,TABLE

SOURCE='snow 雪'
HASH=hashlib.sha256(json.dumps({'source_system':SOURCE,'type_id':1,'id':9223372036854775807},ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
ROW={'source_system':SOURCE,'type_id':'1','id':'9223372036854775807','lookup_hash':HASH,'props_json':'{"wide":18446744073709551615}','entity_version':'2','published_at':'1791486158556147'}
class Pins:
    def __init__(self):self.active=False;self.fail_exit=False
    @contextmanager
    def hold(self,v,context):
        self.active=True
        try:
            yield
            if self.fail_exit:raise PermissionError('Final custody expired')
        finally:self.active=False
class Policy:
    def __init__(self):self.deny_bind=False;self.deny_row=False
    def bind_descriptor(self,d,v,c):
        if self.deny_bind:raise PermissionError('Wrong original custody')
    def authorize_row(self,d,t,r,c):
        if self.deny_row:raise PermissionError('Denied row')
class Executor:
    def __init__(self,pins):self.pins=pins;self.rows=[ROW];self.calls=[];self.uuid='trusted-uuid'
    def query(self,sql,p):
        assert self.pins.active;self.calls.append((sql,p))
        return SQLResult([{'id':self.uuid}]) if sql.startswith('DESCRIBE') else SQLResult(self.rows)
class SingletonTests(unittest.TestCase):
    def setup(self):
        p=Pins();e=Executor(p);b=FakeBackend();policy=Policy();return p,e,b,policy
    def read(self,p,e,b,policy,version=6):
        return read_singleton(e,b,p,PinVector('a','manifest','p1','a'*64,{TABLE:('trusted-uuid',version)}),policy,
            publication_id='p1',table=TABLE,kind='object',source=SOURCE,type_id=1,entity_id=9223372036854775807,context='authorized',supported_profiles=['ashlar-delta/0.3'],supported_revisions={'source':['r1']})
    def test_pinned_bound_unicode_lookup_preserves_wide_text(self):
        p,e,b,policy=self.setup();r=self.read(p,e,b,policy)
        self.assertEqual(r['props_json'],ROW['props_json']);self.assertFalse(p.active)
        sql,params=e.calls[1];self.assertIn('VERSION AS OF 6',sql);self.assertIn('LIMIT 2',sql)
        self.assertNotIn(SOURCE,sql);self.assertEqual(params['hash'],HASH)
        self.assertIn('cast(unix_micros(published_at) AS STRING) AS published_at',sql)
        self.assertEqual(r['published_at'],'1791486158556147')
        with self.assertRaises(TypeError):r['id']='changed'
    def test_timestamp_null_negative_and_signed64_precision_preserved(self):
        for value in [None,'0','-1','9223372036854775807','-9223372036854775808']:
            p,e,b,policy=self.setup();e.rows=[dict(ROW,published_at=value)]
            self.assertEqual(self.read(p,e,b,policy)['published_at'],value)
    def test_truncated_missing_noncanonical_or_narrowed_timestamp_refuses(self):
        for value in ['2026-10-08T19:02:38.556Z','-0','01','1791486158556147.0',1791486158556147,True,'9223372036854775808']:
            p,e,b,policy=self.setup();e.rows=[dict(ROW,published_at=value)]
            with self.assertRaises(ResolutionError):self.read(p,e,b,policy)
            self.assertFalse(p.active)
        p,e,b,policy=self.setup();e.rows=[{k:v for k,v in ROW.items() if k!='published_at'}]
        with self.assertRaises(ResolutionError):self.read(p,e,b,policy)
    def test_absence_and_duplicate_identity(self):
        p,e,b,policy=self.setup();e.rows=[];self.assertIsNone(self.read(p,e,b,policy))
        e.rows=[ROW,ROW]
        with self.assertRaises(ResolutionError):self.read(p,e,b,policy)
    def test_custody_row_and_final_pin_refusals_return_nothing(self):
        for fail in ['bind','row','exit','uuid','version']:
            p,e,b,policy=self.setup()
            if fail=='bind':policy.deny_bind=True
            if fail=='row':policy.deny_row=True
            if fail=='exit':p.fail_exit=True
            if fail=='uuid':e.uuid='replaced'
            with self.assertRaises((PermissionError,ResolutionError)):self.read(p,e,b,policy,7 if fail=='version' else 6)
            self.assertFalse(p.active)
if __name__=='__main__':unittest.main()

class ExpiringSingletonTests(unittest.TestCase):
    setup=SingletonTests.setup
    read=SingletonTests.read
    def test_expiry_during_query_returns_no_result(self):
        p,e,b,policy=self.setup();original=b.validate_descriptor;calls=[]
        def validate(d,c):
            calls.append(1)
            if len(calls)>1:raise PermissionError('Retention window expired during query')
            return original(d,c)
        b.validate_descriptor=validate
        with self.assertRaises(PermissionError):self.read(p,e,b,policy)
        self.assertFalse(p.active);self.assertEqual(len(calls),2)
