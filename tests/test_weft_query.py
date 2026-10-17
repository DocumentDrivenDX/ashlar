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
        q,a,p,e,policy=self.setup()
        events=[]
        original_query=e.query
        original_admit=policy.authorize_query
        original_result=policy.authorize_result
        def query(sql,parameters):
            events.append(('sql',len(e.calls)))
            return original_query(sql,parameters)
        def admit(*args):
            self.assertTrue(p.active)
            events.append(('admit',len(e.calls)))
            return original_admit(*args)
        def authorize_result(*args):
            self.assertTrue(p.active)
            events.append(('result',len(e.calls)))
            return original_result(*args)
        e.query=query;policy.authorize_query=admit;policy.authorize_result=authorize_result
        rows=self.read(q,a,p,e,policy)
        self.assertEqual(rows,(('updated',{'state':'value','value':'雪'}),));self.assertFalse(p.active)
        self.assertEqual(policy.checks,3);self.assertEqual(len(e.calls),3)
        self.assertEqual(events,[('admit',0),('sql',0),('sql',1),('sql',2),('admit',3),('result',3),('admit',3)])
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

    def test_interval_cannot_suppress_original_query_refusal(self):
        from contextlib import contextmanager
        for failure in ['integrity','transport','authority','result']:
            q,a,p,e,policy=self.setup()
            @contextmanager
            def suppress(vector,*,context):
                p.active=True
                try:
                    try:yield
                    except BaseException:pass
                finally:p.active=False
            p.hold=suppress
            if failure=='integrity':e.violate=True
            if failure=='transport':e.fail_type=True
            if failure=='authority':policy.deny_after=True
            if failure=='result':
                def deny(*args):raise PermissionError('Result refused')
                policy.authorize_result=deny
            with self.subTest(failure=failure),self.assertRaises((ResolutionError,PermissionError)):
                self.read(q,a,p,e,policy)
            self.assertFalse(p.active)

    def test_deferred_decoder_never_escapes_held_interval(self):
        q,a,p,e,policy=self.setup();events=[];deferred=[]
        def cold():
            events.append('deferred body');yield 'unprotected'
        def decoder(*args):
            value=cold();deferred.append(value);return value
        policy.decode_result=decoder
        with self.assertRaises(ResolutionError):self.read(q,a,p,e,policy)
        self.assertFalse(p.active);self.assertEqual(events,[])
        self.assertEqual(len(deferred),1);self.assertIsNone(deferred[0].gi_frame)

    def test_deferred_coroutine_decoder_is_closed_without_execution(self):
        q,a,p,e,policy=self.setup();events=[];deferred=[]
        async def cold():
            events.append('deferred body');return 'unprotected'
        def decoder(*args):
            value=cold();deferred.append(value);return value
        policy.decode_result=decoder
        with self.assertRaises(ResolutionError):self.read(q,a,p,e,policy)
        self.assertFalse(p.active);self.assertEqual(events,[])
        self.assertEqual(len(deferred),1);self.assertIsNone(deferred[0].cr_frame)

    def test_nested_deferred_values_close_without_execution_while_held(self):
        q,a,p,e,policy=self.setup();events=[];pending=[]
        def cold():
            events.append('generator ran');yield 'unprotected'
        async def async_cold():
            events.append('coroutine ran');return 'unprotected'
        def decoder(*args):
            self.assertTrue(p.active)
            g,c=cold(),async_cold();pending.extend([g,c])
            return {'outer':[('nested',g),{'value':c}]}
        policy.decode_result=decoder
        with self.assertRaises(ResolutionError):self.read(q,a,p,e,policy)
        self.assertFalse(p.active);self.assertEqual(events,[])
        self.assertIsNone(pending[0].gi_frame);self.assertIsNone(pending[1].cr_frame)

    def test_result_authorization_cannot_insert_deferred_carrier(self):
        q,a,p,e,policy=self.setup();pending=[];events=[]
        def cold():
            events.append('late body');yield 'unprotected'
        def authorize(rows,*args):
            self.assertTrue(p.active);g=cold();pending.append(g);rows[0][1]['late']=[g]
        policy.authorize_result=authorize
        with self.assertRaises(ResolutionError):self.read(q,a,p,e,policy)
        self.assertFalse(p.active);self.assertEqual(events,[]);self.assertIsNone(pending[0].gi_frame)

    def test_completed_data_snapshot_detaches_decoder_and_authorizer_aliases(self):
        from ashlar.weft_query import _completed_decode
        from decimal import Decimal
        from datetime import datetime,timezone
        data={'nested':[bytearray(b'abc'),(Decimal('123.456'),datetime(2026,10,12,tzinfo=timezone.utc))]}
        copied=_completed_decode(data);data['nested'][0][0]=0;data['nested'].append('late')
        self.assertEqual(copied['nested'][0],bytearray(b'abc'));self.assertEqual(len(copied['nested']),2)
        q,a,p,e,policy=self.setup();retained=[];decode=policy.decode_result
        def decoder(*args):
            value=decode(*args)
            if type(value) is dict:retained.append(value)
            return value
        def authorize(rows,*args):retained.append(rows[0][1])
        policy.decode_result=decoder;policy.authorize_result=authorize
        rows=self.read(q,a,p,e,policy)
        for item in retained:item['value']='changed after release'
        self.assertEqual(rows[0][1],{'state':'value','value':'雪'});self.assertFalse(p.active)

    def test_opaque_cycles_depth_and_node_overflow_refuse_without_iteration(self):
        from ashlar.weft_query import _completed_decode
        calls=[]
        class Opaque:
            def __iter__(self):calls.append('iterated');yield 'unprotected'
        cyclic=[];cyclic.append(cyclic)
        deep='leaf'
        for _ in range(65):deep=[deep]
        for value in [Opaque(),cyclic,deep,[None]*100000,{1:'unsupported map key'}]:
            with self.subTest(kind=type(value)),self.assertRaises(ResolutionError):_completed_decode(value)
        self.assertEqual(calls,[])
        class HostileClass:
            @property
            def __class__(self):calls.append('class protocol');return object
        with self.assertRaises(ResolutionError):_completed_decode(HostileClass())
        self.assertEqual(calls,[])
        self.assertEqual(len(_completed_decode([None]*99999)),99999)

    def test_scalar_metadata_cannot_retain_executable_or_mutable_uuid_alias(self):
        from ashlar.weft_query import _completed_decode
        from uuid import UUID
        from datetime import datetime,time,timedelta,timezone
        events=[]
        def cold():events.append('metadata body');yield 'unprotected'
        class Name(str):pass
        g=cold();name=Name('custom');name.payload=g
        opaque_uuid=UUID(int=1,is_safe=g)
        for value in [opaque_uuid,UUID(int=1),datetime(2026,10,12,tzinfo=timezone(timedelta(),name)),time(tzinfo=timezone(timedelta(),name))]:
            with self.assertRaises(ResolutionError):_completed_decode(value)
        self.assertEqual(events,[]);g.close()

    def test_all_recognized_generator_cleanup_attempts_survive_baseexception(self):
        q,a,p,e,policy=self.setup();pending=[];events=[];cleanup_failure=KeyboardInterrupt('first cleanup')
        def started(label):
            try:yield 'staged while held'
            finally:
                events.append((label,p.active))
                if label=='first':raise cleanup_failure
        def decoder(*args):
            self.assertTrue(p.active)
            first,second=started('first'),started('second');next(first);next(second);pending.extend([first,second])
            return {'values':[first,second]}
        policy.decode_result=decoder
        with self.assertRaises(ResolutionError) as failure:self.read(q,a,p,e,policy)
        self.assertIs(failure.exception.__cause__,cleanup_failure)
        self.assertEqual(events,[('first',True),('second',True)]);self.assertFalse(p.active)
        self.assertTrue(all(g.gi_frame is None for g in pending))

    def test_copy_and_visit_budgets_cover_aliases_and_the_complete_read(self):
        from ashlar.weft_query import _completed_decode
        one=bytearray(1024*1024)
        self.assertEqual(len(_completed_decode([one]*8)),8)
        with self.assertRaises(ResolutionError):_completed_decode([one]*9)
        for value in [[one]*4,[None]*60000]:
            q,a,p,e,policy=self.setup();calls=[]
            def decoder(*args):calls.append(True);return value
            policy.decode_result=decoder
            with self.assertRaises(ResolutionError):self.read(q,a,p,e,policy)
            self.assertFalse(p.active);self.assertEqual(len(calls),2)

    def test_closing_failure_preserves_original_authority_refusal(self):
        from contextlib import contextmanager
        q,a,p,e,policy=self.setup();policy.deny_after=True
        @contextmanager
        def fail_close(vector,*,context):
            p.active=True
            try:yield
            finally:
                p.active=False
                raise ResolutionError('Closing failed')
        p.hold=fail_close
        with self.assertRaisesRegex(PermissionError,'Authority expired') as failure:
            self.read(q,a,p,e,policy)
        self.assertIsInstance(failure.exception.__cause__,ResolutionError)
        self.assertFalse(p.active)

    def test_record_integrity_requires_explicit_host_admission_before_sql(self):
        for outcome in ['missing','deny','incomplete','admitted','violation']:
            q,a,p,e,policy=self.setup()
            guards=next(o for o in a['obligations'] if o['id']=='ashlar.candidate.scalarIntegrity')['parameters']['checks']
            guards[:]=[{'sql':guards[0]['sql'],'failureCode':'WFT-BINDING','record':guards[0]['record']}]
            calls=[];phases=[]
            if outcome!='missing':
                def admit(check,artifact,context):
                    self.assertTrue(p.active);calls.append(check);phases.append(len(e.calls))
                    if outcome=='deny':raise PermissionError('Record scope refused')
                    return True if outcome=='incomplete' else None
                policy.admit_record_integrity=admit
            if outcome=='violation':e.violate=True
            if outcome=='admitted':
                self.read(q,a,p,e,policy);self.assertEqual(len(calls),3);self.assertEqual(len(e.calls),2)
                self.assertEqual(phases,[0,2,2])
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

class FinalReleaseAuthorityControls(unittest.TestCase):
    setup=WeftQueryTests.setup
    read=WeftQueryTests.read
    def test_authorize_result_cannot_revoke_profile_or_authority_then_release_buffer(self):
        for failure in ('profile','authority'):
            with self.subTest(failure=failure):
                q,a,p,e,policy=self.setup()
                phases=[]
                def authorize_result(*args):
                    self.assertTrue(p.active)
                    self.assertEqual(policy.checks,2)
                    self.assertEqual(len(e.calls),3)
                    phases.append('result-authorized-then-revoked')
                    if failure=='profile':policy.deny_profile=True
                    else:policy.deny_after=True
                policy.authorize_result=authorize_result
                with self.assertRaises(PermissionError):self.read(q,a,p,e,policy)
                self.assertEqual(phases,['result-authorized-then-revoked'])
                self.assertEqual(len(e.calls),3)
                self.assertEqual(policy.checks,2 if failure=='profile' else 3)
                self.assertFalse(p.active)
