"""Independent byte custody for each actual public pack model operation."""
import base64,hashlib,json,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
class Tests(unittest.TestCase):
    def test_all_original_models_and_absences_are_explicit(self):
        raw=(ROOT/'examples/domain-packs/inventory.json').read_bytes();inventory=json.loads(raw)
        report=json.loads((ROOT/'docs/helix/04-build/evidence/domain-pack-model-validation-20261009.json').read_bytes())
        self.assertEqual(report['inventorySha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(report['packRevision'],inventory['commit'])
        self.assertEqual(len(report['results']),24)
        self.assertEqual([x['pack'] for x in report['results']],[x['directory'] for x in inventory['packs']])
        missing=[]
        for pack,result in zip(inventory['packs'],report['results']):
            op=result['operation']
            if not pack.get('ontology'):
                missing.append(pack['directory']);self.assertEqual(op['status'],'not-available');continue
            original=base64.b64decode(result['sourceBase64'],validate=True)
            file=next(f for f in pack['files'] if f['path']==pack['ontology']['path'])
            self.assertEqual(result['sourceSha256'],hashlib.sha256(original).hexdigest());self.assertEqual(result['sourceSha256'],file['sha256'])
            self.assertEqual(result['sourceBytes'],len(original));self.assertEqual(len(original),file['bytes'])
            self.assertEqual(op['source'],json.loads(original));self.assertEqual(op['source']['umf'],pack['ontology']['umf_version'])
            self.assertEqual(op['status'],'returned');self.assertTrue(op['validation']['valid']);self.assertTrue(op['validation']['complete'])
        self.assertEqual(missing,['gtfs-schedule','movielens','noaa-ghcn-daily','nyc-tlc'])
