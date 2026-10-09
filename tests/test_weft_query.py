import copy,json
from pathlib import Path
import unittest
from ashlar.weft_query import read_weft
from ashlar.pins import PinVector
from ashlar.native import SQLResult
from ashlar.publication import ResolutionError
from test_singleton import Pins
from test_publication import FakeBackend,TABLE

ROOT=Path(__file__).resolve().parents[1]
class Policy:
    def __init__(self,pins):self.pins=pins;self.checks=0;self.deny_after=False;self.deny_profile=False
    def bind_descriptor(self,*args):assert self.pins.active
    def admit_artifact(self,*args):assert self.pins.active
    def verify_native_profile(self,*args):
        if self.deny_profile:raise PermissionError('Native profile unavailable')
    def authorize_query(self,*args):
        self.checks+=1
        if self.deny_after and self.checks>1:raise PermissionError('Authority expired')
    def decode_result(self,column,value,*args):
        if column['representation']['kind']=='scalar':return value
        item=json.loads(value)
        if set(item)!= {'state','value'} or item['state']!='value' or type(item['value']) is not str:raise ResolutionError('Unsupported value envelope')
        return item
    def authorize_result(self,*args):assert self.pins.active
class Executor:
    def __init__(self,artifact,pins):self.a=artifact;self.p=pins;self.calls=[];self.violate=False;self.fail_type=False
    def query(self,sql,params):
        assert self.p.active;self.calls.append((sql,params))
        if sql!=self.a['sql']:return SQLResult([{'violations':'1' if self.violate else '0'}],(('violations','STRING'),))
        return SQLResult([{'label':'updated','caption':'{"state":"value","value":"雪"}'}],(('label','DOUBLE' if self.fail_type else 'STRING'),('caption','STRING')))
class WeftQueryTests(unittest.TestCase):
    def setup(self):
        case=json.loads((ROOT/'docs/helix/04-build/evidence/weft-pinned-compiler-20261008.json').read_text())['compiled']
        a=copy.deepcopy(case['response']);next(o for o in a['obligations'] if o['id']=='ashlar.candidate.publication')['parameters']['publication']['id']='p1'
        p=Pins();return case['request'],a,p,Executor(a,p),Policy(p)
    def read(self,q,a,p,e,policy):
        return read_weft(e,FakeBackend(),p,PinVector('a','manifest','p1','a'*64,{TABLE:('trusted-uuid',6)}),policy,request=q,artifact=a,context='authorized',supported_profiles=['ashlar-delta/0.3'],supported_revisions={'source':['r1']})
    def test_integrity_then_buffered_exact_rows_and_context_close(self):
        q,a,p,e,policy=self.setup();rows=self.read(q,a,p,e,policy)
        self.assertEqual(rows,(('updated',{'state':'value','value':'雪'}),));self.assertFalse(p.active)
        self.assertEqual(policy.checks,2);self.assertEqual(len(e.calls),3)
        self.assertEqual(e.calls[0][1],e.calls[-1][1]);self.assertEqual(e.calls[-1][1]['p2'],'17')
    def test_unknown_obligation_profile_integrity_transport_and_closure_refuse(self):
        for failure in ['unknown','profile','integrity','transport','authority','closure']:
            q,a,p,e,policy=self.setup()
            if failure=='unknown':a['obligations'].append(dict(id='unknown',owner='host',parameters={},failureCode='WFT-OBLIGATION'))
            if failure=='profile':policy.deny_profile=True
            if failure=='integrity':e.violate=True
            if failure=='transport':e.fail_type=True
            if failure=='authority':policy.deny_after=True
            if failure=='closure':p.fail_exit=True
            with self.assertRaises((ResolutionError,PermissionError)):self.read(q,a,p,e,policy)
            self.assertFalse(p.active)
            if failure in ['unknown','profile']:self.assertEqual(e.calls,[])
            if failure=='integrity':self.assertEqual(len(e.calls),1)

    def test_record_integrity_requires_explicit_host_admission_before_sql(self):
        for outcome in ['missing','deny','incomplete','admitted','violation']:
            q,a,p,e,policy=self.setup()
            guards=next(o for o in a['obligations'] if o['id']=='ashlar.candidate.scalarIntegrity')['parameters']['checks']
            guards[:]=[{'sql':guards[0]['sql'],'failureCode':'WFT-BINDING','record':guards[0]['record']}]
            calls=[]
            if outcome!='missing':
                def admit(check,artifact,context):
                    self.assertTrue(p.active);calls.append(check)
                    if outcome=='deny':raise PermissionError('Record scope refused')
                    return True if outcome=='incomplete' else None
                policy.admit_record_integrity=admit
            if outcome=='violation':e.violate=True
            if outcome=='admitted':
                self.read(q,a,p,e,policy);self.assertEqual(len(calls),2);self.assertEqual(len(e.calls),2)
            else:
                with self.assertRaises((ResolutionError,PermissionError)):self.read(q,a,p,e,policy)
                self.assertEqual(len(e.calls),1 if outcome=='violation' else 0)
            self.assertFalse(p.active)

    def test_unsupported_real_count_plan_refuses_before_any_sql_with_empty_result(self):
        import subprocess,sys
        from run_local_example import fixture_inputs
        from ashlar.weft_decode import admit_compiled_count
        from test_weft_decode import CompiledCountTests
        CompiledCountTests.setUpClass()
        intake,semantic,_=fixture_inputs()
        for sql in ["SELECT COUNT(*) AS n FROM Item i WHERE i.label = 'updated'",
                    'SELECT i.label, COUNT(*) AS n FROM Item i GROUP BY i.label ORDER BY i.label LIMIT 10']:
            q,a,p,e,policy=self.setup();q=copy.deepcopy(q);q['sql']=sql
            code='import json,sys;sys.path.insert(0,sys.argv[1]);import weft;print(weft.compile_json(sys.stdin.read()))'
            result=subprocess.run([sys.executable,'-c',code,'/private/tmp/ashlar-weft-python'],input=json.dumps(q),capture_output=True,text=True,check=True)
            a=json.loads(result.stdout);self.assertEqual(a['status'],'compiled')
            next(o for o in a['obligations'] if o['id']=='ashlar.candidate.publication')['parameters']['publication']['id']='p1'
            e.a=a
            def admit(request,artifact,*args):
                for column in artifact['columns']:
                    if column['representation'].get('logicalType',{}).get('family')=='integer':
                        admit_compiled_count(column,artifact,semantic,intake)
            policy.admit_artifact=admit
            # Any execution would return zero rows; admission must refuse first.
            def empty_query(sql,params):
                e.calls.append((sql,params));return SQLResult([],tuple((c['outputName'],'STRING') for c in a['columns']))
            e.query=empty_query
            with self.assertRaises(ResolutionError):self.read(q,a,p,e,policy)
            self.assertEqual(e.calls,[]);self.assertFalse(p.active)
