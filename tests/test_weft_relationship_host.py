import copy,json,os,subprocess,unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
from ashlar.commerce_source import build_transaction
from weft_relationship_plan import relationship_request,admit_relationship_artifact,decode_relationship_rows
from run_commerce_relationship_weft import original_oracle,run_indexed_relationship
ROOT=Path(__file__).resolve().parents[1]

class RelationTests(unittest.TestCase):
    def setUp(self):
        root=ROOT/'examples/domain-packs/commerce/upstream';self.model=(root/'ontology.json').read_bytes();self.graph=(root/'graph/fixture.json').read_bytes()
        _,self.bindings=build_transaction(self.model,self.graph,source_system='private-original-commerce-fixture',binding_profile='ashlar-commerce-development-bindings/0.2')
        names=['object_current','edge_current','tombstone','whole_source_history']
        manifest={'table_versions_json':json.dumps({'fixture.runtime.'+n:0 for n in names}),'source_progress_json':json.dumps({'private-original-commerce-fixture':'1'}),'publication_id':'unpublished-test-fixture'}
        registry=[{'table':'fixture.runtime.'+n,'uuid':'fixture-only-'+n}for n in names+['manifest']]
        aliases={'fixture.runtime.'+n:'spark_catalog.fixture.'+n for n in names}
        self.request=relationship_request(self.model,self.bindings,manifest,registry,aliases)

    def test_original_separate_edge_identity_and_endpoints(self):
        b=json.loads(self.request['target']['bindingJson']);r,=b['relationships']
        self.assertEqual(r['kind'],'edge');self.assertEqual(r['acceptedDefinition']['target'][0]['key'],'identity')
        self.assertEqual(b['publication']['tables'][r['table']]['name'][-1],'edge_current')
        self.assertEqual({v['logical']['element']for v in b['records']},{'products','suppliers'})

    def test_original_oracle_and_exact_carrier_refusals(self):
        graph=json.loads(self.graph)
        self.assertEqual(len(graph['objects']),11);self.assertEqual(len(graph['edges']),10)
        expected=original_oracle(self.graph)
        fixed=[{'id':'[42,0,\"P1\"]','suppliers':{'items':[['[42,0,\"S1\"]']],'truncated':False}}]
        self.assertEqual(expected,fixed)
        relation=[e for e in graph['edges']if e['relationship']=={'document':'urn:umf:domain:commerce','module':'domain','id':'products.supplier_id'}]
        self.assertEqual(len(relation),1)
        bykey={o['key']:o for o in graph['objects']}
        self.assertEqual(bykey[relation[0]['source']]['values']['products.id'],fixed[0]['id'])
        self.assertEqual(bykey[relation[0]['target']]['values']['suppliers.id'],fixed[0]['suppliers']['items'][0][0])
        raw=[{'id':r['id'],'suppliers':json.dumps(r['suppliers'])}for r in expected]
        self.assertEqual(decode_relationship_rows(raw),expected)
        duplicate={'items':[['same'],['same']],'truncated':True}
        self.assertEqual(decode_relationship_rows([{'id':'x','suppliers':json.dumps(duplicate)}])[0]['suppliers'],duplicate)
        for cell in ('{"items":[],"truncated":1}','{"items":[],"truncated":false,"extra":0}','{"items":[],"items":[],"truncated":false}','{"items":[[1]],"truncated":false}'):
            with self.assertRaises(ValueError):decode_relationship_rows([{'id':'x','suppliers':cell}])

    def test_no_schema_callback_before_any_publication_effect(self):
        class Never:
            context=object();provider=object()
            graph=self.graph
        with self.assertRaises(ValueError):run_indexed_relationship(Never(),None)

    def test_actual_artifact_callback_order_schema_and_closing_refusals(self):
        response=(ROOT/'examples/end-to-end/weft-relationship-compiler-fixtures/response.json').read_bytes()
        a=json.loads(response);b=json.loads(self.request['target']['bindingJson'])
        native=[{'id':r['id'],'suppliers':json.dumps(r['suppliers'])}for r in original_oracle(self.graph)]
        resolved=SimpleNamespace(descriptor=SimpleNamespace(raw={'original':True}),snapshots={'unchanged':0})
        for refusal in (None,'schema','guard','close','drift'):
            calls=[]
            class Provider:
                @contextmanager
                def interval(self,context):
                    calls.append('opening');yield
                    calls.append('closing')
                    if refusal=='close':raise ValueError('ACK closing failed')
                def resolve(self,context):
                    if refusal=='drift' and 'user'in calls:return SimpleNamespace(descriptor=SimpleNamespace(raw={'changed':True}),snapshots={'unchanged':1})
                    return resolved
                def admit_binding(self,*args):calls.append('binding')
                def runtime(self,*args):calls.append('runtime')
                def native_table_schema(self,table,*args):
                    names={'id':'BIGINT','source_system':'STRING','schema_revision':'STRING'}
                    names.update({'rel_type_id':'BIGINT','source_type':'BIGINT','source_id':'BIGINT','target_type':'BIGINT','target_id':'BIGINT'}if table['name'][-1]=='edge_current'else{'type_id':'BIGINT','props_json':'STRING'})
                    if refusal=='schema':names['id']='STRING'
                    return {'table':table,'schema':{'fields':[{'name':n,'nullable':True}for n in names]},'nativeTypes':list(names.items())}
                def sql(self,sql,params):
                    if sql==a['sql']:calls.append('user');return native
                    calls.append('guard');return [{'violations':'1'if refusal=='guard'else'0'}]
                def closed_interval_custody(self,*args):return {'closed':True}
            registry=[{'table':'fixture.runtime.'+n,'uuid':'fixture-only-'+n}for n in ['object_current','edge_current','tombstone','whole_source_history','manifest']]
            opened=SimpleNamespace(model=self.model,bindings=self.bindings,graph=self.graph,manifest={'table_versions_json':json.dumps({'fixture.runtime.'+n:0 for n in ['object_current','edge_current','tombstone','whole_source_history']}),'source_progress_json':json.dumps({'private-original-commerce-fixture':'1'}),'publication_id':'unpublished-test-fixture'},original_report={'table_registry':registry},aliases={'fixture.runtime.'+t['name'][-1]:'.'.join(t['name'])for t in b['publication']['tables']},provider=Provider(),context=object(),original_native_files={'f':'hash'},native_files=lambda:{'f':'hash'})
            with patch('run_commerce_relationship_weft.compile_distribution',return_value=response):
                if refusal:
                    with self.assertRaises(ValueError):run_indexed_relationship(opened,None)
                else:
                    result=run_indexed_relationship(opened,None)
                    self.assertEqual(result['decoded'],original_oracle(self.graph))
                    self.assertLess(calls.index('guard'),calls.index('user'));self.assertGreater(calls.index('closing'),calls.index('user'))
            if refusal in ('schema','guard'):self.assertNotIn('user',calls)

    def test_actual_public_compiler_and_mutation_custody(self):
        fixture=ROOT/'examples/end-to-end/weft-relationship-compiler-fixtures'
        import hashlib
        custody=json.loads((fixture/'custody.json').read_bytes())
        for name,key in [('request.json','requestSha256'),('response.json','responseSha256'),('compiler-manifest.json','compilerManifestSha256')]:
            self.assertEqual(hashlib.sha256((fixture/name).read_bytes()).hexdigest(),custody[key])
        self.assertEqual(json.loads((fixture/'request.json').read_bytes()),self.request)
        self.assertEqual(hashlib.sha256(self.model).hexdigest(),custody['originalModelSha256'])
        self.assertEqual(hashlib.sha256(self.graph).hexdigest(),custody['originalGraphSha256'])
        for name,digest in custody['publicSchemas'].items():self.assertEqual(hashlib.sha256((fixture/'public-schemas'/name).read_bytes()).hexdigest(),digest)
        self.assertTrue(all(c['valid']is True for c in json.loads((fixture/'schema-receipt.json').read_bytes())['checks']))
        a=json.loads((fixture/'response.json').read_bytes());self.assertEqual(a['status'],'compiled',a)
        compiler=os.environ.get('ASHLAR_RELATION_COMPILER')
        if compiler:
            fresh=subprocess.run([compiler],input=(fixture/'request.json').read_bytes(),capture_output=True,check=True)
            self.assertEqual(fresh.stdout,(fixture/'response.json').read_bytes())
        self.assertEqual([c['outputName']for c in a['columns']],['id','suppliers'])
        relation_column=a['columns'][1]
        self.assertEqual(relation_column['representation']['kind'],'relatedKeys')
        self.assertEqual(relation_column['representation']['relationship']['relationship'],'products.supplier_id')
        self.assertEqual(relation_column['representation']['key']['fields'][0]['element'],'suppliers.id')
        self.assertEqual(relation_column['representation']['bound'],2)
        self.assertEqual({i['element']for i in relation_column['sourceIdentities']},{'products','suppliers','suppliers.id'})
        admit_relationship_artifact(self.request,a,a)
        wrong=copy.deepcopy(self.request);b=json.loads(wrong['target']['bindingJson']);b['relationships'][0]['target']['element']='products';wrong['target']['bindingJson']=json.dumps(b)
        with self.assertRaises(ValueError):admit_relationship_artifact(wrong,a,a)
        for mutate in (lambda x:x.update(parameters=[]),lambda x:x['obligations'].pop(),lambda x:x['columns'][1]['representation'].update(bound=1),lambda x:x['obligations'].append(copy.deepcopy(x['obligations'][0])),lambda x:x['obligations'][1]['parameters']['checks'].pop()):
            bad=copy.deepcopy(a);mutate(bad)
            with self.assertRaises(ValueError):admit_relationship_artifact(self.request,bad,a)

if __name__=='__main__':unittest.main()
