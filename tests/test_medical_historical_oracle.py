import hashlib,json,shutil,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import medical_historical_oracle as historical
class HistoricalMedicalOracleTests(unittest.TestCase):
    def test_complete_archive_fullvalue_and_original_occurrence_bags(self):
        oracle=historical.historical_oracle();self.assertEqual(oracle['packVersion'],'1.0.0');self.assertEqual(len(oracle['objects']),51);self.assertEqual(len(oracle['edges']),62);self.assertEqual(len(oracle['coordinates']),51)
        tokens=[v for o in oracle['objects']for k,v in o['values'].items()if k in ('patients.active','practitioners.active')];self.assertEqual(tokens,['True','True','True',None]);self.assertEqual(sum(v is None for o in oracle['objects']for v in o['values'].values()),8)
        self.assertIn('no authored revision',oracle['schemaRevisionQualification']);self.assertEqual(oracle['packSha256'],'bc0ab3389b3abd8a54e48b2752e6e08a6b0abfb027c54bf3b9b08c6126bd2e39')
    def test_independent_csv_oracle_catches_graph_value_key_and_incidence_changes(self):
        for change in ['value','key','edge','missing-edge']:
            with self.subTest(change=change),tempfile.TemporaryDirectory()as directory:
                copied=Path(directory)/'medical';shutil.copytree(historical.ROOT,copied);path=copied/'upstream/graph/fixture.json';graph=json.loads(path.read_bytes())
                if change=='value':graph['objects'][0]['values']['resources.native_id']='changed'
                if change=='key':graph['objects'][0]['key']='changed'
                if change=='edge':graph['edges'][0]['target']=graph['edges'][0]['source']
                if change=='missing-edge':graph['edges'].pop()
                path.write_text(json.dumps(graph))
                with patch.object(historical,'ROOT',copied):
                    with self.assertRaises((ValueError,KeyError)):historical.historical_oracle()
    def test_regenerated_original_oracle_matches_frozen_public_input_hash(self):
        root=Path(__file__).resolve().parents[1];evidence=root/'docs/helix/04-build/evidence/medical-historical-admission-20261009';raw=evidence.joinpath('original-oracle.json').read_bytes();self.assertEqual(raw,(json.dumps(historical.historical_oracle(),ensure_ascii=False)+'\n\n').encode())
        result=json.loads(evidence.joinpath('public-receipt.json').read_bytes());self.assertEqual(hashlib.sha256(raw).hexdigest(),result['originalOracleSha256']);self.assertEqual(result['umfRevision'],'8e76c74d14203225d1ef159c9132bb9e9b0cdffe');self.assertEqual(result['receipt']['datasetValidation'],{'valid':True,'complete':True,'diagnostics':[]})
        self.assertEqual([r['request']['token']for r in result['booleanReceipts']],['True']*3);self.assertTrue(all(r['value']=={'boolean':True}and r['validation']=={'valid':True,'complete':True,'diagnostics':[]}and r['provenance']=='unverified'for r in result['booleanReceipts']))
        self.assertTrue(all(c['receipt']['datasetValidation']['valid']is False for c in result['controls']));self.assertEqual(len(result['controls']),4)
if __name__=='__main__':unittest.main()
