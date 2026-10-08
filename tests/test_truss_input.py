import base64
import copy
import hashlib
import json
from pathlib import Path
import unittest
from ashlar.schema import SchemaIntake
from ashlar.truss_input import AcceptanceInputError, artifact, exact_artifact, verify_acceptance_input_custody, _canonical

ROOT=Path(__file__).resolve().parents[1]
PIN='16c35e8d943769ccfa7bb57d16785aa7159abe65'

class TrussInputTests(unittest.TestCase):
    def setUp(self):
        self.intake=SchemaIntake.read((ROOT/'examples/end-to-end/schema-v3.intake.json').read_bytes(),'3',trusted_validator_revision=PIN)
        self.archive=b'{"status":"test-only-unqualified-profile"}'
        self.pin={'identity':'fixture','version':'1','sha256':hashlib.sha256(self.archive).hexdigest()}
        self.input={'interfaceVersion':'truss-acceptance-input/0.1.0',**{k:copy.deepcopy(self.pin) for k in ['layoutProfile','acceptanceProfile','validatorProfile','supportProfile']},
            'documents':[{'documentId':self.intake.document_id,'documentRevision':'3','artifact':artifact('original-source',self.intake.source),'umfProfile':self.pin,'ingress':{'kind':'native'}}],
            'binding':{'state':'absent'},'policy':{'unknownEndpoint':'reject','loss':'strict','profile':self.pin},'transforms':[]}
    def verify(self,value,**overrides):
        args={'profile_archives':{('fixture','1'):self.archive},'intakes':[self.intake],'trusted_validator_revision':PIN,'document_order':(self.intake.document_id,)};args.update(overrides)
        raw=value if type(value) is bytes else json.dumps(value,ensure_ascii=False).encode()
        return verify_acceptance_input_custody(raw,**args)
    def test_independent_complete_wire_byte_vectors(self):
        vectors=json.loads((ROOT/'tests/fixtures/truss-lineage/canonical-input-v0.1.vectors.json').read_text())['vectors']
        self.assertEqual(len(vectors),4)
        for vector in vectors:
            with self.subTest(name=vector['name']):
                token=_canonical(json.loads(vector['canonicalUtf8']))
                self.assertEqual(token.encode().hex(),vector['canonicalUtf8Hex'])
                preimage=('truss-canonical/0.1.0\n'+vector['domain']+'\n'+token).encode()
                self.assertEqual(hashlib.sha256(preimage).hexdigest(),vector['sha256'])
        # Published shape-only acceptance uses dummy profile pins: custody refuses it.
        with self.assertRaises(AcceptanceInputError):self.verify(vectors[0]['canonicalUtf8'].encode())

    def test_complete_custody_and_exact_semantic_repeat(self):
        first=self.verify(self.input)
        raw=json.dumps(self.input,sort_keys=True,separators=(',',':')).encode()
        second=self.verify(raw)
        self.assertNotEqual(first.original,second.original)
        self.assertTrue(first.exact_repeat(second))
        self.assertEqual(first.document_sources,(self.intake.source,))
        self.assertTrue(first.canonical_preimage.startswith(b'truss-canonical/0.1.0\ntruss-acceptance-input/0.1.0\n'))
        changed=copy.deepcopy(self.input);changed['policy']['loss']='report'
        self.assertFalse(first.exact_repeat(self.verify(changed)))
    def test_changed_binding_or_ordered_transform_meaning_is_not_repeat(self):
        original=self.verify(self.input)
        altered=copy.deepcopy(self.input)
        altered['binding']={'state':'present','vocabulary':self.pin,'artifact':artifact('binding',b'{"home":"json"}')}
        self.assertFalse(original.exact_repeat(self.verify(altered)))
        altered['transforms']=[{'registration':self.pin,'targetDefinitionIdentity':name,'parameters':{'null':None,'flag':False,'text':'a\nb'}} for name in ['a','b']]
        first=self.verify(altered)
        self.assertIn(b'a\\u000ab',first.canonical_preimage)
        altered['transforms'].reverse()
        self.assertFalse(first.exact_repeat(self.verify(altered)))
    def test_refuses_source_revision_archive_members_and_numeric_drift(self):
        mutations=[lambda v:v.update(origin={}),lambda v:v['documents'][0].update(documentRevision='4'),
            lambda v:v['documents'][0]['artifact'].update(sha256='0'*64),
            lambda v:v['validatorProfile'].update(sha256='0'*64),
            lambda v:v['documents'].append(copy.deepcopy(v['documents'][0])),
            lambda v:v.update(transforms={}),lambda v:v.update(transforms=[{'registration':self.pin,'targetDefinitionIdentity':'x','parameters':1}])]
        for mutate in mutations:
            value=copy.deepcopy(self.input);mutate(value)
            with self.subTest(value=str(value)[:80]):
                with self.assertRaises(AcceptanceInputError):self.verify(value)
        with self.assertRaises(AcceptanceInputError):self.verify(self.input,profile_archives={})
        with self.assertRaises(AcceptanceInputError):self.verify(self.input,document_order=('other',))
        with self.assertRaises(ValueError):self.verify(b'{"documents":[],"documents":[]}')
    def test_noncanonical_base64_and_unknown_artifact_members_refuse(self):
        # Both encodings decode to the same byte; unused pad bits must be zero.
        value=artifact('binary',b'f');value['bytesBase64']='Zh=='
        with self.assertRaises(AcceptanceInputError):exact_artifact(value)
        value=artifact('binary',b'f');value['extra']='hidden'
        with self.assertRaises(AcceptanceInputError):exact_artifact(value)

if __name__=='__main__':unittest.main()
