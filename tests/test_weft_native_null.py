"""Exact tagged output controls plus optional actual public compiler replay inputs."""
import copy,json,os,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from run_commerce_arithmetic_weft import decode_rows,execute_guarded
from weft_field_plan import admit_field_plan

class NativeNullDecoderTests(unittest.TestCase):
    def artifact(self,family='string',facets=None):
        identity={'documentId':'urn:test','module':'domain','element':'optional','revision':'original'}
        return {'columns':[{'position':1,'outputName':'v','nullable':False,'sourceIdentities':[identity], 'representation':{'kind':'value','descriptor':identity,'nativeNull':True}}], 'logicalPlan':{'typeGraph':[{'identity':identity,'availability':'absent-allowed','kind':'scalar','type':{'family':family,'facets':facets or {},'nullable':False}}]}}
    def decode(self,value,family='string',facets=None):
        return decode_rows(self.artifact(family,facets),[{'v':value}],native_null=True)
    def test_null_and_values_are_distinct(self):
        self.assertEqual(self.decode('{"state":"null"}'),[[{'state':'null'}]])
        for raw,family,value in [('""','string',''),('false','boolean',False),('"0"','integer','0'),('"-0.00"','decimal','-0.00')]:
            self.assertEqual(self.decode('{"state":"value","value":'+raw+'}',family,{'precision':18,'scale':2} if family=='decimal' else None),[[{'state':'value','value':value}]])
    def test_unknown_duplicate_coercion_and_absent_refuse(self):
        for raw in ['null','{}','{"state":"absent"}','{"state":"null","value":null}','{"state":"null","state":"value"}','{"state":"value","value":null}','{"state":"value","value":1}']:
            with self.assertRaises(ValueError):self.decode(raw)
        for raw in ['{"state":"value","value":"false"}','{"state":"value","value":0}']:
            with self.assertRaises(ValueError):self.decode(raw,'boolean')
        with self.assertRaises(ValueError):decode_rows(self.artifact(),[{'v':'{"state":"null"}'}])
    def test_decimal_exact_range_and_scale(self):
        for value in ['1.001','10000000000000000.00','1e0','NaN']:
            with self.assertRaises(ValueError):self.decode(json.dumps({'state':'value','value':value}),'decimal',{'precision':18,'scale':2})
        for mutate in [lambda a:a['logicalPlan']['typeGraph'][0]['type'].update(facets={'unknown':True}),lambda a:a['columns'][0]['representation'].update(nativeNull=1),lambda a:a['logicalPlan']['typeGraph'][0].update(availability='required')]:
            a=self.artifact();mutate(a)
            with self.assertRaises(ValueError):decode_rows(a,[{'v':'{"state":"null"}'}],native_null=True)
    def test_old_default_field_facets_are_not_physical_slot_facets(self):
        import hashlib
        from test_weft_field_plan import FieldPlanAdmissionTests
        request,artifact,binding=FieldPlanAdmissionTests().fixture('specialists')
        source=json.loads(request['modules'][0]['documentJson'])
        field=next(e for m in source['modules']for e in m['elements']if e['id']=='fauna_results.nisp')
        field['facets']={'integerWidth':{'bits':64,'signed':True}}
        text=json.dumps(source);old=request['modules'][0]['pin']['sha256'];new=hashlib.sha256(text.encode()).hexdigest()
        def repin(value):
            if type(value)is dict:return{k:repin(v)for k,v in value.items()}
            if type(value)is list:return[repin(v)for v in value]
            return new if value==old else value
        request,artifact,binding=map(repin,(request,artifact,binding));request['modules'][0]['documentJson']=text
        with self.assertRaisesRegex(ValueError,'Original default Field facets required'):
            admit_field_plan(artifact,binding,request['modules'])

    def test_capability_refuses_before_callback_without_optin(self):
        # Target mismatch also refuses first; no callback may run on unknown input.
        class Provider:
            def interval(self,context):raise AssertionError('callback ran')
        request={'target':{'bindingJson':'{}','bindingSha256':'wrong'}}
        with self.assertRaises(ValueError):execute_guarded(Provider(),request,{'logicalPlan':{'requiredCapabilities':['predicate.nativeNull']}},context={})

@unittest.skipUnless(os.environ.get('ASHLAR_NULL_COMPILER_FIXTURES'),'Explicit actual compiler artifacts not supplied')
class OriginalNullCompilerTests(unittest.TestCase):
    def test_original_four_and_adversarial_home_guard_proofs(self):
        root=Path(os.environ['ASHLAR_NULL_COMPILER_FIXTURES'])
        for case in ['archaeology-sample','ecology-censor','ecology-effort','ecology-zero']:
            request=json.loads((root/(case+'-request.json')).read_bytes());artifact=json.loads((root/(case+'-artifact.json')).read_bytes());binding=json.loads(request['target']['bindingJson'])
            admit_field_plan(artifact,binding,request['modules'],native_null=True)
            with self.assertRaises(ValueError):admit_field_plan(artifact,binding,request['modules'])
            class NoCallbacks:
                def interval(self,context):raise AssertionError('unexpected native callback')
            with self.assertRaises(ValueError):execute_guarded(NoCallbacks(),request,artifact,context={})
            for kind in ('flag','capability'):
                changed_artifact=copy.deepcopy(artifact)
                if kind=='flag':
                    tagged=[c for c in changed_artifact['columns']if c['representation']['kind']=='value']
                    if not tagged:continue
                    tagged[0]['representation']['nativeNull']=1
                else:changed_artifact['logicalPlan']['requiredCapabilities'].remove('value.nativeNull')
                with self.assertRaises(ValueError):execute_guarded(NoCallbacks(),request,changed_artifact,context={},native_null=True)
            changed=copy.deepcopy(binding)
            for r in changed['records']:
                for p in r['properties']:
                    if p['home'].get('encoding')=='ashlar-weft-json-native-null/0.1-candidate':p['home']['encoding']='json'
            with self.assertRaises(ValueError):admit_field_plan(artifact,changed,request['modules'],native_null=True)
            changed=copy.deepcopy(artifact)
            checks=next(o for o in changed['obligations']if o['id']=='ashlar.candidate.scalarIntegrity')['parameters']['checks']
            checks[:]=[c for c in checks if not(c.get('encoding')=='ashlar-weft-json-native-null/0.1-candidate'and c.get('representabilityOnly')is True)]
            with self.assertRaises(ValueError):admit_field_plan(changed,binding,request['modules'],native_null=True)

if __name__=='__main__':unittest.main()
