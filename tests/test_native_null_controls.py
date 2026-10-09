import copy,json,sys,unittest,uuid
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from prepare_native_null_controls import fixture_model,controls,request,ENCODING
class NativeNullControlInputs(unittest.TestCase):
    def test_distinct_source_states_and_separate_model(self):
        model=fixture_model();self.assertEqual(model['umf'],'0.8.0');self.assertTrue(model['id'].startswith('urn:ashlar:authored:'))
        cases={c['name']:c for c in controls()};self.assertEqual(len(cases),6)
        self.assertNotIn('amount',json.loads(cases['missing-optional']['originalPropsJson']))
        self.assertIsNone(json.loads(cases['explicit-null']['originalPropsJson'])['amount'])
        original=json.loads(cases['zero-false-empty']['originalPropsJson']);self.assertEqual(original['text'],'');self.assertIs(original['flag'],False);self.assertEqual(original['zero'],0)
        self.assertEqual(cases['required-null']['expectedDisposition'],'source-invalid')
        self.assertEqual(cases['missing-optional']['expectedDisposition'],'capability')
    def test_actual_vector_inputs_closed_and_home_permissions_explicit(self):
        vector=[{'name':['spark_catalog','control',str(n)],'uuid':str(uuid.uuid4()),'version':0}for n in range(4)]
        r=request(fixture_model(),controls()[0]['sql'],vector,str(uuid.uuid4()));binding=json.loads(r['target']['bindingJson'])
        self.assertEqual(binding['publication']['tables'],vector)
        props=binding['records'][0]['properties']
        self.assertEqual({p['logical']['element']for p in props if p['home'].get('encoding')==ENCODING},{'text','flag','amount'})
        for key,value in [('version',True),('uuid','invented'),('name',['too','many','name','parts'])]:
            changed=copy.deepcopy(vector);changed[0][key]=value
            with self.assertRaises(ValueError):request(fixture_model(),'SELECT id FROM Item',changed,str(uuid.uuid4()))
if __name__=='__main__':unittest.main()
