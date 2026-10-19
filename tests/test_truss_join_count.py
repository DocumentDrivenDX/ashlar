# SPDX-License-Identifier: MIT OR Apache-2.0
"""Public observed join IR and accepted-home controls; no native execution."""
import copy,json,unittest
import test_truss_weft_related as fixtures
from ashlar_host.truss_join_count import admit_truss_join_count

class Tests(unittest.TestCase):
    def fixture(self):
        admission,artifact,binding,expected=fixtures.Tests().fixture()
        record=next(r for r in binding['records']if r['logical']['element']=='B')
        field=next(p['logical']for p in record['properties']if p['logical']['element']=='B-value')
        string={'family':'string','facets':{},'nullable':False};integer={'family':'integer','facets':{},'nullable':False}
        pin=binding['modelPins'][0];typed=lambda scan:{'identity':field,'scan':scan,'span':{'start':1,'end':2},'type':string}
        plan=artifact['logicalPlan'];plan.update(aggregate=True,filters=[],groups=[],order=[],typeGraph=[],source={'occurrence':'s0','pin':pin,'record':record['logical']},
            joins=[{'right':{'occurrence':'s1','pin':pin,'record':record['logical']},'on':[{'op':'equal','left':typed('s0'),'right':{'kind':'field','field':typed('s1')}}]}],
            outputs=[{'name':'n','expression':{'op':'count','type':integer}}],requiredCapabilities=['aggregate','aggregate.count','equal','innerJoin','project','scan','type.string'])
        artifact.update(status='compiled',interfaceVersion='weft-compile/0.2.0',dialect='weft-sql/0.2.0',compilerVersion='weft/0.1.0')
        artifact['columns']=[{'nullable':False,'outputName':'n','position':1,'representation':{'kind':'scalar','logicalType':integer,'carrier':'text','decoder':'exact-integer'},'sourceIdentities':[record['logical'],record['logical']]}]
        return admission,artifact,binding,expected,record
    def run_case(self,admission,artifact,binding,expected):
        return admit_truss_join_count(admission,artifact,binding,expected,revision='1',document_id='cohort')
    def test_duplicate_matching_pairs_and_nonmatching_values(self):
        a,artifact,binding,expected,record=self.fixture()
        original=next(r for r in expected['object_current']if r['type_id']==record['typeId'])
        fields={p['logical']['element']:p['home']['propertyId']for p in record['properties']}
        props=json.loads(original['props_json']);props[fields['B-label']]='different-key'
        expected['object_current'].append(dict(original,id='20',props_json=json.dumps(props)))
        props[fields['B-value']]='nonmatching';props[fields['B-label']]='third-key'
        expected['object_current'].append(dict(original,id='21',props_json=json.dumps(props)))
        self.assertEqual(self.run_case(a,artifact,binding,expected),[{'n':5}])
    def test_foreign_home_field_profile_and_occurrence_refuse(self):
        for change in ('home','field','profile','occurrence'):
            a,artifact,binding,expected,record=self.fixture()
            if change=='home':next(p for p in record['properties']if p['logical']['element']=='B-value')['home']['propertyId']='999'
            elif change=='field':artifact['logicalPlan']['joins'][0]['on'][0]['left']['identity']=dict(artifact['logicalPlan']['joins'][0]['on'][0]['left']['identity'],element='foreign')
            elif change=='profile':artifact['interfaceVersion']='weft-compile/9.0.0'
            else:artifact['logicalPlan']['joins'][0]['right']['occurrence']='s0'
            with self.subTest(change=change),self.assertRaises(ValueError):self.run_case(a,artifact,binding,expected)
