"""Exact original commerce finite-dataset receipt, independently checked endpoints."""
import hashlib,json,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original=ROOT/'examples/domain-packs/commerce/upstream'
        cls.report=json.loads((ROOT/'docs/helix/04-build/evidence/commerce-public-dataset-20261009.json').read_bytes())
        cls.source=json.loads((cls.original/'ontology.json').read_bytes());cls.graph=json.loads((cls.original/'graph/fixture.json').read_bytes())
    def test_original_declared_version_and_entire_source_remain_unchanged(self):
        d=self.report;r=d['receipt'];self.assertEqual(r['source'],self.source);self.assertEqual(r['source']['umf'],'0.8.0')
        for file,field in [('ontology.json','sourceSha256'),('graph/fixture.json','graphSha256')]:self.assertEqual(d[field],hashlib.sha256((self.original/file).read_bytes()).hexdigest())
        self.assertEqual(r['datasetValidation'],{'valid':True,'complete':True,'diagnostics':[]})
        self.assertEqual(r['scope'],'supplied-dataset-only');self.assertEqual(r['provenance'],'unverified')
    def test_every_original_instance_endpoint_and_key_occurrence_is_retained(self):
        r=self.report['receipt'];self.assertEqual(len(r['records']),11);self.assertEqual(len(r['keys']),11);self.assertEqual(len(r['relationships']),10)
        self.assertEqual({x['instanceId'] for x in r['records']},{x['key'] for x in self.graph['objects']})
        actual={x['instanceId']:(x['sourceInstanceId'],x['targetInstanceId']) for x in r['relationships']}
        self.assertEqual(actual,{x['key']:(x['source'],x['target']) for x in self.graph['edges']})
        for row in r['records']:
            self.assertTrue(row['result']['validation']['valid']);self.assertFalse(row['result']['validation']['complete'])
            self.assertEqual({x['code'] for x in row['result']['validation']['diagnostics']},{'RECORD_KEY_CONTEXT_REQUIRED','RECORD_RELATIONSHIP_CONTEXT_REQUIRED'})
    def test_adversarial_inputs_do_not_close_dataset_admission(self):
        controls=self.report['controls'];self.assertEqual([c['name'] for c in controls],['duplicate-key','unresolved-target','missing-required-relationship'])
        for control in controls:
            self.assertFalse(control['receipt']['datasetValidation']['valid'])
            self.assertEqual(control['receipt']['source'],self.source)
            self.assertEqual(control['receipt']['input'],control['input'])
