import hashlib
import json
from pathlib import Path
import unittest
from ashlar.lineage import lineage_bytes, LineageError, TYPE, RELATIONSHIP

class LineageTests(unittest.TestCase):
    def test_independent_truss_vectors(self):
        files=list((Path(__file__).parent/'fixtures/truss-lineage').glob('*.json'))
        self.assertEqual(len(files),2)
        for file in files:
            fixture=json.loads(file.read_text())
            self.assertEqual(len(fixture['vectors']),6)
            for vector in fixture['vectors']:
                with self.subTest(name=vector['name'], profile=fixture['identityTransportProfile']):
                    raw=lineage_bytes(fixture['identityTransportProfile'],vector['identity'])
                    self.assertEqual(raw.hex(),vector['storedBytesHex'])
                    self.assertEqual(str(len(raw)),vector['storedBytesLength'])
                    self.assertEqual(hashlib.sha256(raw).hexdigest(),vector['routingSha256'])

    def test_control_spelling_and_no_normalization(self):
        self.assertTrue(lineage_bytes(TYPE,['d','m','a\nb']).endswith(b'["d","m","a\\u000ab"]'))
        self.assertNotEqual(lineage_bytes(TYPE,['d','m','é']),lineage_bytes(TYPE,['d','m','e\u0301']))

    def test_refuses_unknown_incomplete_and_untransportable(self):
        cases=[(TYPE,['d','m']), (TYPE,['d','m',True]), (TYPE,['d','m','\ud800']),
            (TYPE,['d','m','a\x00b']), (TYPE,['d','m','x'*180000]), ('unknown',['d','m','e']),
            (RELATIONSHIP,{'category':'authored','relationship':{'document':'d','module':'m','element':'e'},'extra':'x'}),
            (RELATIONSHIP,{'category':'composition_field','profile':'unknown','ownerRecord':{},'sourceField':{}})]
        for profile,tree in cases:
            with self.subTest(tree=str(tree)[:100]):
                with self.assertRaises(LineageError):lineage_bytes(profile,tree)
