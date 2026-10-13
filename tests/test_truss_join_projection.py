import copy,json,unittest
import test_truss_join_count as fixtures
from ashlar_host.truss_join_count import admit_truss_join_projection

class Tests(unittest.TestCase):
    def fixture(self):
        a,artifact,binding,expected,record=fixtures.Tests().fixture()
        plan=artifact['logicalPlan'];plan['aggregate']=False;plan['requiredCapabilities']=['equal','innerJoin','project','scan','type.string']
        string={'family':'string','facets':{},'nullable':False};outputs=[];columns=[];identities=[]
        for position,(name,scan,field)in enumerate((('left_label','s0','B-label'),('right_value','s1','B-value')),1):
            identity=next(p['logical']for p in record['properties']if p['logical']['element']==field)
            outputs.append({'name':name,'expression':{'op':'field','scan':scan,'identity':identity}})
            columns.append({'nullable':False,'outputName':name,'position':position,'representation':{'kind':'scalar','logicalType':string,'carrier':'text','decoder':'text'},'sourceIdentities':[identity]})
            identities.append(identity)
        plan['outputs']=outputs;plan['typeGraph']=[{'availability':'required','identity':identity,'kind':'scalar','type':string}for identity in identities];artifact['columns']=columns
        return a,artifact,binding,expected,record
    def run_case(self,a,artifact,binding,expected):
        return admit_truss_join_projection(a,artifact,binding,expected,revision='1',document_id='cohort')
    def test_matching_pair_bag_retains_duplicates_and_sparse_nonmatch(self):
        a,artifact,binding,expected,record=self.fixture();row=next(r for r in expected['object_current']if r['type_id']==record['typeId'])
        homes={p['logical']['element']:p['home']['propertyId']for p in record['properties']};props=json.loads(row['props_json'])
        props[homes['B-label']]='second';expected['object_current'].append(dict(row,id='20',props_json=json.dumps(props)))
        props[homes['B-label']]='third';props[homes['B-value']]='different';expected['object_current'].append(dict(row,id='21',props_json=json.dumps(props)))
        rows=self.run_case(a,artifact,binding,expected)
        self.assertCountEqual(rows,[{'left_label':'exact-B-label','right_value':'exact-B-value'}]*2+[{'left_label':'second','right_value':'exact-B-value'}]*2+[{'left_label':'third','right_value':'different'}])
    def test_foreign_projection_home_occurrence_and_typegraph_refuse(self):
        for change in ('home','occurrence','typegraph'):
            a,artifact,binding,expected,record=self.fixture()
            if change=='home':next(p for p in record['properties']if p['logical']['element']=='B-label')['home']['propertyId']='999'
            elif change=='occurrence':artifact['logicalPlan']['outputs'][0]['expression']['scan']='s2'
            else:artifact['logicalPlan']['typeGraph']=[]
            with self.subTest(change=change),self.assertRaises(ValueError):self.run_case(a,artifact,binding,expected)
