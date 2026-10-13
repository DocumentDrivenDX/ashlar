"""Grouped IR shape observed from public Weft0.2; inert catalog/oracle controls."""
import copy,json,unittest
import test_truss_graph_admission as cases
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_weft_query import truss_string_compile_request,admit_truss_grouped_count,_execute_string_artifact
from ashlar_host.delta_custody import encoded

class Tests(unittest.TestCase):
    def test_original_field_identity_group_bag_and_refusal(self):
        batch,catalogs=cases.Tests().original();admission=TrussGraphAdmission(batch,catalogs)
        installation=json.loads(batch.begin)['prior']['installationId'];tables={f'local.truss.{role}':0 for role in ('object_current','edge_current','tombstone','whole_source_history')}
        manifest={'publication_id':'original','profile_version':'ashlar-delta/0.3','recorded_at':'1','table_versions_json':encoded(tables),'schema_revisions_json':encoded({installation:'1'}),'source_progress_json':'{"feed":{"position":"1"}}','validation_report_json':encoded({'complete':True,'source_admission':admission.metadata()})}
        uuids={table:'00000000-0000-0000-0000-'+str(index).zfill(12)for index,table in enumerate(tables,1)}
        request=truss_string_compile_request(admission,manifest,uuids,'00000000-0000-0000-0000-000000000005','SELECT a."A-value" AS label, COUNT(*) AS n FROM A a GROUP BY a."A-value"',revision='1',document_id='cohort')
        binding=json.loads(request['target']['bindingJson']);pin=binding['modelPins'][0];record=next(item for item in binding['records']if item['logical']['element']=='A');field=next(item for item in record['properties']if item['logical']['element']=='A-value')
        string={'family':'string','facets':{},'nullable':False};integer={'family':'integer','facets':{},'nullable':False}
        descriptor=lambda logical,decoder:{'kind':'scalar','logicalType':logical,'carrier':'text','decoder':decoder}
        artifact={'status':'compiled','interfaceVersion':'weft-compile/0.2.0','dialect':'weft-sql/0.2.0','compilerVersion':'weft/0.1.0','modelPins':[pin],'logicalPlan':{'aggregate':True,'filters':[],'groups':[{'identity':field['logical'],'scan':'s0','span':{'start':61,'end':72},'type':string}],
            'irVersion':'weft-ir/0.2.0','joins':[],'limit':None,'modulePins':[pin],'order':[],
            'outputs':[{'name':'label','expression':{'op':'field','scan':'s0','identity':field['logical']}},{'name':'n','expression':{'op':'count','type':integer}}],
            'pageKey':None,'readProfile':None,'requiredCapabilities':['aggregate','aggregate.count','group','project','scan','type.string'],
            'source':{'occurrence':'s0','pin':pin,'record':record['logical']},'typeGraph':[{'availability':'required','identity':field['logical'],'kind':'scalar','type':string}]},
            'columns':[{'position':1,'outputName':'label','nullable':False,'representation':descriptor(string,'text'),'sourceIdentities':[field['logical']]},{'position':2,'outputName':'n','nullable':False,'representation':descriptor(integer,'exact-integer'),'sourceIdentities':[record['logical']]}]}
        row={'source_system':installation,'type_id':record['typeId'],'schema_revision':'1'}
        # Equal non-key values with distinct labels preserve the original keys.
        expected={'object_current':[dict(row,id=str(index),props_json=encoded({item['home']['propertyId']:'same' if item['logical']['element']=='A-value' else label for item in record['properties']}))for index,label in ((1,'one'),(2,'two'))]}
        self.assertEqual(admit_truss_grouped_count(admission,artifact,binding,expected,revision='1',document_id='cohort'),[{'label':'same','n':2}])
        for mutation in ('filter','field','source','home','profile'):
            changed=copy.deepcopy(artifact);mapping=copy.deepcopy(binding)
            if mutation=='filter':changed['logicalPlan']['filters']=[{}]
            elif mutation=='field':changed['logicalPlan']['groups'][0]['identity']['element']='B-label'
            elif mutation=='source':next(item for item in mapping['records']if item['logical']==record['logical'])['sourceSystem']='foreign'
            elif mutation=='home':next(item for item in next(item for item in mapping['records']if item['logical']==record['logical'])['properties']if item['logical']==field['logical'])['home']['propertyId']='foreign'
            else:changed['interfaceVersion']='weft-compile/0.3.0'
            with self.assertRaises(ValueError):admit_truss_grouped_count(admission,changed,mapping,expected,revision='1',document_id='cohort')
        artifact.update(parameters=[],sql='WITH original SELECT original',obligations=[{'id':'ashlar.candidate.scalarIntegrity','parameters':{'checks':[]}}])
        observed={'schema':[['label','STRING'],['n','STRING']],'rows':[['same','2'],['other','1']]}
        with patch('ashlar_host.path_capture.capture_string_frame',return_value=observed):
            rows,_=_execute_string_artifact(SimpleNamespace(sql=lambda *args,**kwargs:None),artifact,3,grouped=True)
            self.assertEqual(rows,[{'label':'same','n':2},{'label':'other','n':1}])
            observed['rows']=[['same','-1']]
            with self.assertRaises(ValueError):_execute_string_artifact(SimpleNamespace(sql=lambda *args,**kwargs:None),artifact,3,grouped=True)
