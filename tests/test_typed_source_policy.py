import base64,copy,hashlib,json,subprocess,tempfile,unittest
from pathlib import Path
from ashlar.apply import Change,EntityKey,EntityState
from ashlar.typed_source_policy import TypedSourcePolicy,TypedSourceError,typed_values,RECORD_CHECK_PIN
ROOT=Path(__file__).resolve().parents[1]
SOURCE=Path('/private/tmp/ashlar-umf-records-c45c72a2')
FIELDS={'21':('m','quantity','integer'),'22':('m','amount','decimal'),'23':('m','active','boolean'),'24':('m','note','string')}
BINDINGS={17:{'record':('m','Item'),'fields':FIELDS}}

def source():
    members=[{'module':'m','element':x} for x in ['quantity','amount','active','note']]
    fields=[{'id':'quantity','kind':'field','scalarType':'integer','cardinality':'one','nullability':'required','facets':{'integerWidth':{'bits':64,'signed':False}},'extensions':{}},
            {'id':'amount','kind':'field','scalarType':'decimal','cardinality':'one','nullability':'required','facets':{'precision':18,'scale':2},'extensions':{}},
            {'id':'active','kind':'field','scalarType':'boolean','cardinality':'one','nullability':'required','extensions':{}},
            {'id':'note','kind':'field','scalarType':'string','cardinality':'one','nullability':'absent-allowed','extensions':{}}]
    return json.dumps({'umf':'0.8.0','id':'typed-source-example','vocabularies':{},'modules':[{'id':'m','namespace':'urn:typed:','elements':[{'id':'Item','kind':'record','members':members,'extensions':{}},*fields]}]},separators=(',',':')).encode()

def change(props):
    digest=hashlib.sha256(props.encode()).hexdigest()
    return Change('typed-feed','epoch','delivery',digest,'create',EntityState(EntityKey('typed-source','object',17,1),1,'1',props,'{"opaque":18446744073709551615}'))

def request(c):
    return {'deliveryId':c.delivery_id,'recordSha256':c.raw_digest,'typeId':17,'propsSha256':hashlib.sha256(c.state.props_json.encode()).hexdigest(),'identity':{'module':'m','element':'Item'},'values':typed_values(c.state.props_json,FIELDS)}

class TypedSourcePolicyTests(unittest.TestCase):
    def setUp(self):
        if not SOURCE.exists():self.skipTest('Pinned public UMF producer absent; native public record check not executed')
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.model=source()

    def run_check(self,c,model=None):
        root=Path(self.temp.name);(root/'model.json').write_bytes(self.model if model is None else model)
        (root/'request.json').write_text(json.dumps({'records':[request(c)]},separators=(',',':')))
        return subprocess.run(['bun',str(ROOT/'tools/check_typed_umf_records.ts'),str(SOURCE),str(root/'model.json'),str(root/'request.json')],capture_output=True)

    def policy(self,c):
        p=self.run_check(c);self.assertEqual(p.returncode,0,p.stderr.decode())
        return TypedSourcePolicy(self.model,p.stdout,BINDINGS,source_system='typed-source',schema_revision='1',trusted_producer_revision=RECORD_CHECK_PIN),p.stdout

    def test_exact_scalar_and_presence_public_checks(self):
        for props in ['{"21":18446744073709551615,"22":123.4500,"23":true}',
                      '{"21":9007199254740993,"22":-0.00,"23":false,"24":null}',
                      '{"21":10,"22":1.250e1,"23":true,"24":"雪"}']:
            c=change(props);policy,receipt=self.policy(c);self.assertIsNone(policy(c));self.assertEqual(policy.request(c),request(c))
            result=json.loads(receipt);self.assertEqual(base64.b64decode(result['sourceBase64']),self.model)
            self.assertEqual(c.state.props_json,props)
            literals={v['field']['element']:v['value'] for v in result['records'][0]['result']['values']}
            self.assertEqual(literals['quantity']['integerToken'],props.split(':',1)[1].split(',',1)[0])

    def test_actual_umf_domains_presence_and_types_refuse(self):
        for props in ['{"21":18446744073709551616,"22":1.00,"23":true}',
                      '{"21":1,"22":1.001,"23":true}',
                      '{"21":1,"22":1.00}',
                      '{"21":true,"22":1.00,"23":true}',
                      '{"21":1,"22":1.00,"23":"true"}']:
            with self.subTest(props=props):self.assertNotEqual(self.run_check(change(props)).returncode,0)

    def test_changed_custody_binding_and_incomplete_receipt_refuse(self):
        c=change('{"21":1,"22":12.50,"23":true}');policy,raw=self.policy(c)
        with self.assertRaises(TypedSourceError):policy(change('{"21":1,"22":12.51,"23":true}'))
        incomplete_document=json.loads(raw)
        incomplete_document['records'][0]['result']['documentValidation']['complete']=False
        incomplete_document['records'][0]['result']['documentValidation']['diagnostics']=[{'severity':'warning','code':'original-experimental-warning','message':'Retained producer warning'}]
        TypedSourcePolicy(self.model,json.dumps(incomplete_document).encode(),BINDINGS,source_system='typed-source',schema_revision='1',trusted_producer_revision=RECORD_CHECK_PIN)
        for mutation in ['source','producer','request','complete','identity','values','order','missing-fields','field-order','duplicate-field','field-state','field-invalid','document-shape','aggregate-numeric-valid','aggregate-numeric-complete','field-numeric-valid','field-numeric-complete']:
            a=json.loads(raw)
            if mutation=='source':a['sourceSha256']='0'*64
            if mutation=='producer':a['producerRevision']='0'*40
            if mutation=='request':a['requestSha256']='0'*64
            if mutation=='complete':a['records'][0]['result']['validation']['complete']=False
            if mutation=='identity':a['records'][0]['result']['identity']['element']='quantity'
            if mutation=='values':a['records'][0]['result']['values']=[]
            if mutation=='order':a['records']=[]
            if mutation=='missing-fields':a['records'][0]['result']['fields']=[]
            if mutation=='field-order':a['records'][0]['result']['fields'].reverse()
            if mutation=='duplicate-field':a['records'][0]['result']['fields'][1]=copy.deepcopy(a['records'][0]['result']['fields'][0])
            if mutation=='field-state':a['records'][0]['result']['fields'][0]['state']='absent'
            if mutation=='field-invalid':a['records'][0]['result']['fields'][0]['validation']['complete']=False
            if mutation=='document-shape':a['records'][0]['result']['documentValidation'].pop('diagnostics')
            if mutation.startswith('aggregate-numeric-'):a['records'][0]['result']['validation'][mutation.rsplit('-',1)[1]]=1
            if mutation.startswith('field-numeric-'):a['records'][0]['result']['fields'][0]['validation'][mutation.rsplit('-',1)[1]]=1
            with self.subTest(mutation=mutation),self.assertRaises(TypedSourceError):TypedSourcePolicy(self.model,json.dumps(a).encode(),BINDINGS,source_system='typed-source',schema_revision='1',trusted_producer_revision=RECORD_CHECK_PIN)

    def test_transport_refuses_unknown_duplicate_nested_without_private_semantics(self):
        for props in ['{"99":1}','{"21":1,"21":2}','{"21":{"integerToken":"1"}}','{"21":NaN}','{"21":[1]}']:
            with self.subTest(props=props),self.assertRaises(TypedSourceError):typed_values(props,FIELDS)
        self.assertEqual(typed_values('{"21":1e2,"22":-0.00}',FIELDS),[{'field':{'module':'m','element':'quantity'},'state':'present','value':{'integerToken':'1e2'}},{'field':{'module':'m','element':'amount'},'state':'present','value':{'decimalToken':'-0.00'}}])

    def test_keys_relationships_and_unknown_extensions_do_not_become_complete(self):
        c=change('{"21":1,"22":1.00,"23":true}')
        a=json.loads(self.model);a['modules'][0]['elements'][0]['keys']=[{'id':'identity','fields':[{'module':'m','element':'quantity'}],'primary':True}]
        self.assertNotEqual(self.run_check(c,json.dumps(a).encode()).returncode,0)
        a=json.loads(self.model);a['modules'][0]['elements'][0]['extensions']={'unknown.meaning':{'native':'retained'}}
        self.assertNotEqual(self.run_check(c,json.dumps(a).encode()).returncode,0)
