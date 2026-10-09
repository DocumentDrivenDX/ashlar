import json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from medical_source_oracle import ROOT,original_oracle,current_dataset_input,pointer
class OriginalMedicalOracleTests(unittest.TestCase):
    def test_exact_resource_reference_and_authored_types(self):
        oracle=original_oracle()
        self.assertEqual(len(oracle['resourceBytes']),17);self.assertEqual(len(oracle['references']),17)
        self.assertEqual(sum(map(len,oracle['tables'].values())),51)
        self.assertTrue(all(r['exactBytes']is True for r in oracle['resourceBytes']))
        self.assertEqual([r['originalResourceState']for r in oracle['booleanCarriers']],['present','present','present','absent'])
        self.assertTrue(all(r['originalScalarType']=='string'for r in oracle['authoredStringFields']))
        self.assertEqual(oracle['tables']['observations'][0]['value_decimal'],'6.3')
        self.assertEqual(oracle['tables']['observations'][0]['effective_start'],'2013-04-02T09:30:10+01:00')
    def test_explicit_current_csv_carriers_keep_native_null_and_boolean(self):
        input,coordinates=current_dataset_input();self.assertEqual(len(coordinates),51);self.assertEqual(len(input['relationships']),62)
        records=input['records'];active=[v['value']for r in records if r['identity']['element']in ('patients','practitioners')for v in r['values']if v['field']['element'].endswith('.active')]
        self.assertEqual(active,[{'boolean':True},{'boolean':True},{'boolean':True},None])
        reference=next(r for r in records if r['identity']['element']=='resource_references');self.assertEqual(reference['values'][0]['value'],{'integerToken':'1'})
        observation=next(r for r in records if r['identity']['element']=='observations')
        self.assertEqual(next(v['value']for v in observation['values']if v['field']['element']=='observations.value_decimal'),{'string':'6.3'})
        self.assertEqual(next(v['value']for v in observation['values']if v['field']['element']=='observations.effective_end'),None)
    def test_json_pointer_keeps_native_reference_without_resolution_invention(self):
        value={'x/y':[{'~key':{'reference':'Patient/f001'}}]}
        self.assertEqual(pointer(value,'/x~1y/0/~0key/reference'),'Patient/f001')
        with self.assertRaises((KeyError,IndexError)):pointer(value,'/x~1y/0/unknown')
        with self.assertRaises(ValueError):pointer(value,'Patient/f001')
    def test_historical_pack_archive_and_boolean_tokens_stay_separate(self):
        import hashlib,zipfile
        custody=json.loads((ROOT/'source-custody.json').read_bytes());current=json.loads((ROOT/'upstream/pack.json').read_bytes());historical=json.loads((ROOT/'historical/original-pack.json').read_bytes())
        self.assertEqual(current['version'],'1.1.0');self.assertEqual(historical['version'],'1.0.0');self.assertNotEqual(custody['currentPackSha256'],custody['historicalPackSha256'])
        graph=json.loads((ROOT/'upstream/graph/fixture.json').read_bytes());self.assertEqual(graph['pack']['sha256'],custody['historicalPackSha256'])
        with zipfile.ZipFile(ROOT/'historical/original-1.0.0.zip')as z:self.assertEqual(z.read('schemas/ontology.json'),(ROOT/'historical/archive/schemas/ontology.json').read_bytes())
        tokens=[v for o in graph['objects']for k,v in o['values'].items()if k in ('patients.active','practitioners.active')]
        self.assertEqual(tokens,['True','True','True',None])

if __name__=='__main__':unittest.main()
