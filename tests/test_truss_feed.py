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
