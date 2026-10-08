import hashlib,json,unittest
from ashlar.truss_input import artifact,_canonical
from ashlar.truss_feed import assemble_feed,FeedAssemblyError

class Policy:
    def __init__(self):self.completed=0
    def admit_manifest(self,raw,context):
        if context!='fixture':raise PermissionError('No native source authorization')
    def admit_complete(self,raw,payloads,context):self.completed+=1
class FeedAssemblyTests(unittest.TestCase):
    def fixture(self):
        payloads=[artifact('original-a',b'opaque 18446744073709551615'),artifact('original-b','雪'.encode())]
        pin={'identity':'fixture','version':'1','sha256':'a'*64}
        manifest={'interfaceVersion':'truss-feed-transaction/0.1.0','context':{'sourceEpoch':'original','feedProfile':'fixture','scopeIdentity':'scope'},'xid':'9007199254740993','manifestProfile':pin,'configurationPrerequisites':[], 'members':[{'ordinal':str(i),'key':{'kind':'change','seq':str(i)},'payloadProfile':pin,'payloadSha256':p['sha256']} for i,p in enumerate(payloads)],'prerequisites':[]}
        manifest['manifestSha256']=hashlib.sha256(b'truss-canonical/0.1.0\ntruss-feed-manifest/0.1.0\n'+_canonical(manifest).encode()).hexdigest()
        original=artifact('original-manifest',_canonical(manifest).encode())
        fragments=[json.dumps({'manifest':manifest,'records':[{'ordinal':str(i),'payload':p}]},ensure_ascii=False).encode() for i,p in reversed(list(enumerate(payloads)))]
        return original,fragments
    def test_complete_order_original_bytes_and_fragment_replay(self):
        original,fragments=self.fixture();policy=Policy()
        result=assemble_feed(original,fragments+[fragments[0]],policy=policy,context='fixture')
        self.assertEqual(result.ordered_payloads,(b'opaque 18446744073709551615','雪'.encode()))
        self.assertEqual(result.original_fragments,tuple(fragments+[fragments[0]]));self.assertEqual(policy.completed,1)
    def test_missing_tampered_foreign_or_changed_manifest_refuses(self):
        original,fragments=self.fixture()
        for delta in ['missing','digest','ordinal','manifest']:
            changed=list(fragments)
            if delta=='missing':changed=changed[:1]
            else:
                value=json.loads(changed[0])
                if delta=='digest':value['records'][0]['payload']=artifact('changed',b'changed')
                if delta=='ordinal':value['records'][0]['ordinal']='999'
                if delta=='manifest':value['manifest']['xid']='other'
                changed[0]=json.dumps(value).encode()
            policy=Policy()
            with self.assertRaises(FeedAssemblyError):assemble_feed(original,changed,policy=policy,context='fixture')
            self.assertEqual(policy.completed,0)
    def test_unadmitted_source_never_assembles(self):
        original,fragments=self.fixture()
        with self.assertRaises(PermissionError):assemble_feed(original,fragments,policy=Policy(),context='unadmitted')
if __name__=='__main__':unittest.main()

class OriginalFeedVectorTests(unittest.TestCase):
    def vectors(self):
        from pathlib import Path
        return json.loads((Path(__file__).parent/'fixtures/truss-feed-manifest-original-vectors.json').read_bytes())['vectors']
    def test_original_v01_wire_reaches_mandatory_admission_without_hash_rewrite(self):
        vector=self.vectors()[0];raw=bytes.fromhex(vector['completeWireUtf8Hex'])
        self.assertEqual(raw.decode(),vector['completeWireCanonicalToken'])
        self.assertEqual(hashlib.sha256(raw).hexdigest(),vector['completeWireArtifactSha256'])
        class OriginalPolicy:
            called=False
            def admit_manifest(self,original,context):
                self.called=True
                if original!=raw:raise AssertionError('Original archive altered')
                raise PermissionError('Original synthetic profiles not natively admitted')
            def admit_complete(self,*args):raise AssertionError('Unadmitted source completed')
        policy=OriginalPolicy()
        with self.assertRaises(PermissionError):assemble_feed(artifact('original-upstream-vector',raw),[],policy=policy,context='unadopted')
        self.assertTrue(policy.called)
    def test_exact_upstream_canonical_and_framed_bytes_for_all_domains(self):
        for vector in self.vectors():
            raw=bytes.fromhex(vector['canonicalUtf8Hex']);tree=json.loads(raw)
            self.assertEqual(_canonical(tree).encode(),raw)
            self.assertEqual(len(raw),int(vector['canonicalByteLength']))
            framed=b'truss-canonical/0.1.0\n'+vector['domain'].encode()+b'\n'+raw
            self.assertEqual(framed,bytes.fromhex(vector['framedPreimageHex']))
            self.assertEqual(hashlib.sha256(framed).hexdigest(),vector['manifestSha256'])
            self.assertNotEqual(vector['manifestSha256'],vector['directTreeSha256'])
    def test_v02_originals_refuse_before_any_admission(self):
        class NoPolicy:
            def admit_manifest(self,*args):raise AssertionError('Wrong version reached policy')
        for vector in self.vectors()[1:]:
            with self.assertRaises(FeedAssemblyError):assemble_feed(artifact('original-v02',bytes.fromhex(vector['completeWireUtf8Hex'])),[],policy=NoPolicy(),context='unadopted')
    def test_preimage_archive_or_unframed_hash_cannot_replace_complete_manifest(self):
        vector=self.vectors()[0]
        class NoPolicy:
            def admit_manifest(self,*args):raise AssertionError('Invalid archive reached policy')
        preimage=bytes.fromhex(vector['canonicalUtf8Hex'])
        with self.assertRaises(FeedAssemblyError):assemble_feed(artifact('preimage-only',preimage),[],policy=NoPolicy(),context='unadopted')
        tree=json.loads(bytes.fromhex(vector['completeWireUtf8Hex']));tree['manifestSha256']=vector['directTreeSha256']
        with self.assertRaises(FeedAssemblyError):assemble_feed(artifact('wrong-hash-domain',_canonical(tree).encode()),[],policy=NoPolicy(),context='unadopted')

class PrerequisiteCustodyTests(unittest.TestCase):
    def test_rehashed_manifest_does_not_hide_corrupt_original_prerequisite(self):
        from ashlar.truss_input import AcceptanceInputError
        from pathlib import Path
        vector=json.loads((Path(__file__).parent/'fixtures/truss-feed-manifest-original-vectors.json').read_bytes())['vectors'][0]
        tree=json.loads(bytes.fromhex(vector['completeWireUtf8Hex']))
        tree['prerequisites'][0]['artifact']['bytesBase64']='Y2hhbmdlZA=='
        tree.pop('manifestSha256')
        tree['manifestSha256']=hashlib.sha256(b'truss-canonical/0.1.0\ntruss-feed-manifest/0.1.0\n'+_canonical(tree).encode()).hexdigest()
        class NoPolicy:
            def admit_manifest(self,*args):raise AssertionError('Corrupt prerequisite reached admission')
        with self.assertRaises(AcceptanceInputError):assemble_feed(artifact('rehashed',_canonical(tree).encode()),[],policy=NoPolicy(),context='unadopted')
