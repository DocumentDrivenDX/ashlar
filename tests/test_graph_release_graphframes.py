import copy,hashlib,json,unittest
from ashlar.graph_release import _carrier
from test_graph_release import carrier
from run_graph_release_graphframes import load_release,oracle,columns,graph_rows,row_bag

def fixture():
    return {'format':'ashlar-graph-release/0.1',
            'publication':{'publication_id':'p1','profile_version':'ashlar-delta/0.3','recorded_at':'1',
                           'table_versions_json':'{"n":5,"e":0}','schema_revisions_json':'{"source":"r1"}',
                           'source_progress_json':'{}','validation_report_json':json.dumps({'complete':True,'retention':{'targets':{'n':{'uuid':'00000000-0000-0000-0000-000000000001','version':5},'e':{'uuid':'00000000-0000-0000-0000-000000000002','version':0}}}})},
            'roles':{'nodes':'n','edges':'e'},
            'snapshots':{'n':{'uuid':'00000000-0000-0000-0000-000000000001','version':5},'e':{'uuid':'00000000-0000-0000-0000-000000000002','version':0}},
            'mapping':{'identity':'ashlar-key/1','properties':'Exact text','residuals':['Preserved bags'],
                       'capabilities':{'reversibleIdentity':True,'independentEdges':True,'isolatedNodes':True,'exactCanonicalText':True,'selectedScalarPromotion':False,'nativeReleaseMaterialization':False,'engineExecution':False},
                       'losses':[],'engineSupport':[]},
            'nodes':[_carrier(carrier('node',i),'node') for i in ['1','2','3']],
            'edges':[_carrier(carrier('edge',i,target=t),'edge') for i,t in [('7','2'),('8','2'),('9','1')]]}

def encoded(v):return (json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
def admit(v):
    b=encoded(v);return load_release(b,hashlib.sha256(b).hexdigest())

class Tests(unittest.TestCase):
    def test_exact_properties_identity_and_directed_multigraph_oracle(self):
        v=admit(fixture());self.assertEqual(oracle(v),{'nodes':3,'edges':3,'one_hop':3,'two_hop':3,'isolates':1})
        mapped=graph_rows(v['nodes']);self.assertEqual(mapped[0]['carrier_id'],'1')
        self.assertEqual(mapped[0]['id'],v['nodes'][0]['graph_id'])
        self.assertEqual(mapped[0]['props_json'],v['nodes'][0]['props_json'])
        self.assertEqual(row_bag(mapped),row_bag(reversed(mapped)))
    def test_empty_and_explicit_columns(self):
        v=fixture();v['nodes']=[];v['edges']=[];self.assertEqual(oracle(admit(v))['two_hop'],0)
        self.assertIn('graph_id',columns('node'));self.assertIn('src',columns('edge'))
    def test_refusal_custody_encoding_closed_carriers_duplicates_endpoints(self):
        for failure in ['sha','encoding','extra','missing','identity','duplicate-node','duplicate-edge','dangling','integer','hash','null']:
            v=fixture()
            if failure=='encoding':v['mapping']['identity']='unknown'
            if failure=='extra':v['nodes'][0]['extra']='surprise'
            if failure=='missing':del v['nodes'][0]['props_json']
            if failure=='identity':v['nodes'][0]['graph_id']='forged'
            if failure=='duplicate-node':v['nodes'].append(copy.deepcopy(v['nodes'][0]))
            if failure=='duplicate-edge':v['edges'].append(copy.deepcopy(v['edges'][0]))
            if failure=='dangling':v['edges'][0]=_carrier(carrier('edge','7',target='999'),'edge')
            if failure=='integer':v['nodes'][0]['id']='01'
            if failure=='hash':v['nodes'][0]['lookup_hash']='0'*64
            if failure=='null':v['nodes'][0]['props_json']=None
            b=encoded(v)
            with self.subTest(failure=failure),self.assertRaises(ValueError):
                load_release(b,'0'*64 if failure=='sha' else hashlib.sha256(b).hexdigest())
    def test_duplicate_json_and_noncanonical_bytes(self):
        for b in [b'{"format":"x","format":"y"}',json.dumps(fixture()).encode()]:
            with self.assertRaises(ValueError):load_release(b,hashlib.sha256(b).hexdigest())

    def test_original_manifest_lineage_and_complete_mapping_refusals(self):
        mutations=[lambda v:v.pop('publication'),lambda v:v.update(extra='unknown'),
                   lambda v:v['publication'].pop('source_progress_json'),
                   lambda v:v['publication'].update(table_versions_json='{"n":4,"e":0}'),
                   lambda v:v['snapshots']['n'].update(uuid='forged'),
                   lambda v:v['snapshots']['n'].update(uuid='00000000-0000-0000-0000-000000000099'),
                   lambda v:v['snapshots']['n'].update(version=True),
                   lambda v:v['snapshots'].pop('e'),
                   lambda v:v['roles'].update(edges='n'),lambda v:v['roles'].update(edges='absent'),
                   lambda v:v['mapping'].pop('losses'),lambda v:v['mapping'].pop('capabilities'),
                   lambda v:v['mapping']['capabilities'].update(engineExecution=True),
                   lambda v:v['mapping'].update(losses=['dropped precision'])]
        for i,mutate in enumerate(mutations):
            v=fixture();mutate(v)
            with self.subTest(mutation=i),self.assertRaises(ValueError):admit(v)
