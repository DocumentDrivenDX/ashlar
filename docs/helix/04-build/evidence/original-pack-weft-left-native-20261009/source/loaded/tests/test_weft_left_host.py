"""Actual pinned compiler descriptors; unit callbacks grant no native authority."""
import copy,json,unittest
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from run_commerce_arithmetic_weft import execute_guarded,decode_rows,NativeGuardRefusal
from weft_left_plan import admit_left_plan,admit_left_cells,admit_left_schema
ROOT=Path(__file__).resolve().parents[1]/'examples/end-to-end/weft-left-compiler-fixtures'

class Provider:
    def __init__(self,artifact):self.calls=[];self.artifact=artifact;self.bad_type=False;self.bad_close=False;self.violation=False
    @contextmanager
    def interval(self,context):
        self.calls.append('opening');yield;self.calls.append('closed')
        if self.bad_close:raise ValueError('Native closing failure')
    def resolve(self,context):self.calls.append('resolve');return SimpleNamespace(descriptor=SimpleNamespace(raw={'original':'unit-only'}),snapshots={})
    def admit_binding(self,*args):self.calls.append('binding')
    def runtime(self,*args):self.calls.append('runtime')
    def native_table_schema(self,table,*args):
        self.calls.append('schema');return {'table':table,'schema':{'type':'struct','fields':[{'name':'id','type':'string' if self.bad_type else 'long','nullable':True,'metadata':{}}]},'nativeTypes':[['id','STRING' if self.bad_type else 'BIGINT']]}
    def sql(self,sql,params):
        self.calls.append(('guard',sql));return [{'violations':'1' if self.violation else '0'}]
    def sql_ordered(self,sql,params):
        self.calls.append(('user',sql));return {'schema':[['author','STRING'],['form','STRING'],['taxon','STRING']],'rows':[['Author A','{"state":"value","value":"bowl"}','{"state":"absent"}']]}

class LeftHostTests(unittest.TestCase):
    def setUp(self):
        self.request=json.loads((ROOT/'original-request.json').read_bytes());self.artifact=json.loads((ROOT/'original-artifact.json').read_bytes());self.binding=json.loads(self.request['target']['bindingJson'])
    def test_original_membership_and_selected_module_refuse_before_callbacks(self):
        import hashlib
        request=copy.deepcopy(self.request);artifact=copy.deepcopy(self.artifact)
        model=json.loads(request['modules'][0]['documentJson'])
        record=next(e for m in model['modules']for e in m['elements']if e['id']=='pottery_results')
        record['members']=[m for m in record['members']if m['element']!='pottery_results.form']
        source=json.dumps(model,separators=(',',':'));old=request['modules'][0]['pin']['sha256'];new=hashlib.sha256(source.encode()).hexdigest()
        request['modules'][0]['documentJson']=source
        request=json.loads(json.dumps(request).replace(old,new));artifact=json.loads(json.dumps(artifact).replace(old,new))
        digest=hashlib.sha256(request['target']['bindingJson'].encode()).hexdigest();request['target']['bindingSha256']=digest;artifact['bindingSha256']=digest
        p=Provider(artifact)
        with self.assertRaisesRegex(ValueError,'qualified Record member'):execute_guarded(p,request,artifact,context=object(),left_join=True)
        self.assertEqual(p.calls,[])
        request=copy.deepcopy(self.request);request['modules'][0]['selectedModuleIds']=[];p=Provider(self.artifact)
        with self.assertRaises(ValueError):execute_guarded(p,request,self.artifact,context=object(),left_join=True)
        self.assertEqual(p.calls,[])
    def test_complete_slots_and_actual_repeated_scan_inventory_before_callbacks(self):
        for slots in ([],self.artifact['parameters'][:-1],self.artifact['parameters'][1:],self.artifact['parameters']+self.artifact['parameters'][-1:]):
            artifact=copy.deepcopy(self.artifact);artifact['parameters']=copy.deepcopy(slots)
            for i,slot in enumerate(artifact['parameters'],1):slot['position']=i
            p=Provider(artifact)
            with self.assertRaises(ValueError):execute_guarded(p,self.request,artifact,context=object(),left_join=True)
            self.assertEqual(p.calls,[])
        request=json.loads((ROOT/'self-join-request.json').read_bytes());artifact=json.loads((ROOT/'self-join-artifact.json').read_bytes())
        binding=json.loads(request['target']['bindingJson']);admit_left_plan(artifact,binding,request['modules'])
        origins=[p['origin']for p in artifact['parameters']if p['origin']['kind']=='sourceDiscriminator']
        self.assertEqual(len(origins),2);self.assertEqual(origins[0],origins[1])
        artifact['parameters']=artifact['parameters'][:len(artifact['parameters'])//2]
        p=Provider(artifact)
        with self.assertRaises(ValueError):execute_guarded(p,request,artifact,context=object(),left_join=True)
        self.assertEqual(p.calls,[])
    def test_original_real_descriptors_reach_ordered_transport_after_all_guards(self):
        p=Provider(self.artifact);result=execute_guarded(p,self.request,self.artifact,context=object(),left_join=True)
        self.assertEqual(len(result['left_match_schemas']),2)
        guards=[c['check']['sql'] for c in result['checks']]
        self.assertEqual([x[1] for x in p.calls if type(x)is tuple and x[0]=='guard'],guards)
        self.assertEqual(p.calls[-1],'closed');self.assertEqual([x for x in p.calls if type(x)is tuple and x[0]=='user'],[('user',self.artifact['sql'])])
        decoded=decode_rows(self.artifact,result['rows'],native_schema=result['native_schema'],ordered_rows=result['ordered_rows'],left_join=True)
        self.assertEqual(decoded,[['Author A',{'state':'value','value':'bowl'},{'state':'absent'}]])
    def test_missing_schema_callback_refuses_before_interval_or_other_callbacks(self):
        p=Provider(self.artifact);p.native_table_schema=None
        with self.assertRaises(ValueError):execute_guarded(p,self.request,self.artifact,context=object(),left_join=True)
        self.assertEqual(p.calls,[])
    def test_wrong_actual_type_empty_schema_refuses_before_any_guard_or_user_sql(self):
        p=Provider(self.artifact);p.bad_type=True
        with self.assertRaises(ValueError):execute_guarded(p,self.request,self.artifact,context=object(),left_join=True)
        self.assertFalse(any(type(x)is tuple for x in p.calls))
    def test_guard_or_closing_failure_never_releases_result(self):
        p=Provider(self.artifact);p.violation=True
        with self.assertRaises(NativeGuardRefusal):execute_guarded(p,self.request,self.artifact,context=object(),left_join=True)
        self.assertFalse(any(type(x)is tuple and x[0]=='user' for x in p.calls))
        p=Provider(self.artifact);p.bad_close=True
        with self.assertRaises(ValueError):execute_guarded(p,self.request,self.artifact,context=object(),left_join=True)
    def test_scan_record_caps_permissions_and_numeric_flags_refuse_before_callbacks(self):
        mutations=[]
        a=copy.deepcopy(self.artifact);a['columns'][1]['representation']['outerJoin']['scan']='s3';mutations.append(a)
        a=copy.deepcopy(self.artifact);a['columns'][1]['representation']['outerJoin']['record']=a['logicalPlan']['source']['record'];mutations.append(a)
        a=copy.deepcopy(self.artifact);a['columns'][1]['representation']['nativeNull']=0;mutations.append(a)
        a=copy.deepcopy(self.artifact);a['logicalPlan']['requiredCapabilities'].remove('join.left');mutations.append(a)
        a=copy.deepcopy(self.artifact);a['logicalPlan']['outerJoinScans'].reverse();mutations.append(a)
        a=copy.deepcopy(self.artifact);next(o for o in a['obligations'] if o['id']=='outerJoin.matchIntegrity')['parameters']['scans'].pop();mutations.append(a)
        for artifact in mutations:
            p=Provider(artifact)
            with self.assertRaises(ValueError):execute_guarded(p,self.request,artifact,context=object(),left_join=True)
            self.assertEqual(p.calls,[])
        with self.assertRaises(ValueError):execute_guarded(Provider(self.artifact),self.request,self.artifact,context=object())
    def test_full_lexical_bags_and_distinct_null_absence_states(self):
        observed={'schema':[['author','STRING'],['form','STRING'],['taxon','STRING']],'rows':[['','{"state":"value","value":""}','{"state":"absent"}'],['','{"state":"value","value":""}','{"state":"absent"}']]}
        result=admit_left_cells(self.artifact,observed);self.assertEqual(len(result['decoded']),2);self.assertEqual(result['decoded'][0],['',{'state':'value','value':''},{'state':'absent'}])
        for cell in ['{"state":"null"}','{"state":"unknown"}','{"state":"value","value":false}','{"state":"absent","extra":0}','{"state":"absent","state":"value"}']:
            bad=copy.deepcopy(observed);bad['rows'][0][1]=cell
            with self.assertRaises(ValueError):admit_left_cells(self.artifact,bad)
    def test_optional_permission_removal_and_unselected_right_guard_removal_refuse(self):
        binding=copy.deepcopy(self.binding)
        for r in binding['records']:
            for p in r['properties']:p['home'].pop('encoding',None)
        with self.assertRaises(ValueError):admit_left_plan(self.artifact,binding,self.request['modules'])
        request=json.loads((ROOT/'unselected-right-request.json').read_bytes());artifact=json.loads((ROOT/'unselected-right-artifact.json').read_bytes());binding=json.loads(request['target']['bindingJson'])
        self.assertEqual(len(admit_left_plan(artifact,binding,request['modules'])['scans']),2)
        next(o for o in artifact['obligations'] if o['id']=='outerJoin.matchIntegrity')['parameters']['scans']=[]
        with self.assertRaises(ValueError):admit_left_plan(artifact,binding,request['modules'])

if __name__=='__main__':unittest.main()
