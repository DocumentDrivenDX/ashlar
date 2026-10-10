import gzip,hashlib,json
from pathlib import Path
def desc(p):
 p=Path(p);b=p.read_bytes();return dict(path=str(p),sha256=hashlib.sha256(b).hexdigest(),bytes=len(b))
parent=Path('/private/tmp/astra-paths-original-snapshots-parent-review-20261010-a.json');child=Path('/private/tmp/astra-original21-path-five-review-20261010-a.json')
assert desc(parent)['sha256']=='4254f59e2ce0fe09364969b9c4e20554e39735d3f57c72bfcfe4c82f264733d3'
assert desc(child)['sha256']=='620568d2628f7dc61b18d0b4d2cee9d7aa06dd5e5b969c2aae9f55f8d97999bd'
a=json.loads(parent.read_bytes());b=json.loads(child.read_bytes())
rows=[json.loads(x) for x in gzip.decompress(Path('/Users/erik/Projects/ashlar/docs/helix/04-build/evidence/weft-path-corpus-inputs-20261010/retained-exact-case-pairs.jsonl.gz').read_bytes()).splitlines()]
accepted={}
for r in a['accepted']+b['accepted']:
 i=r['index'];assert i not in accepted
 q=bytes.fromhex(rows[i]['requestHex']);v=bytes.fromhex(rows[i]['responseHex'])
 assert hashlib.sha256(q).hexdigest()==r['requestSha256']
 assert hashlib.sha256(v).hexdigest()==r.get('expectedResponseSha256',r.get('fullStdoutSha256'))
 assert len(q)==r['requestBytes'] and len(v)==r.get('expectedResponseBytes',r.get('fullStdoutBytes'))
 accepted[i]=dict(index=i,id=rows[i]['id'],requestSha256=hashlib.sha256(q).hexdigest(),requestBytes=len(q),expectedResponseSha256=hashlib.sha256(v).hexdigest(),expectedResponseBytes=len(v),status=json.loads(v)['status'])
assert sorted(accepted)==list(range(21));assert len({r['requestSha256'] for r in accepted.values()})==19
out=Path('/private/tmp/astra-paths-original21-expected-acceptance-20261010-a.json')
v=dict(verdict='ACCEPT exact retained21 original raw responses as reviewed finite regression expectations',parentReview=desc(parent),childReview=desc(child),accepted=[accepted[i] for i in range(21)],counts=dict(observations=21,uniqueRequests=19,compiled=17,blocked=4,independentMutationRefusals=len(a['mutationRefusals'])+len(b['mutations'])),source='Original compiler-generated observations accepted only after separate source, contract, complete IR/SQL/metadata correspondence and finite logical-oracle review; no expectation bytes changed.',limits=['No new compiler replay or native execution in this review.','Existing native evidence remains tied to its original binaries, runtime profiles and exact inputs. New530 binary is not given inherited native qualification.','Valid empty-root collection is witnessed by a min0 relationship fixture; commerce min=max1 matched-root emptiness is a source-integrity refusal. LEFT absence and post-expansion empty grouping remain separate.','Acceptance of original21 plus separately reviewed nine observations does not replace the required complete47-capability map, whole fresh corpus replay, exact463 namespace controls,19 static controls and8 transport controls.','Finite mutation controls and logical graphs are not an unbounded formal proof, trusted package registration or native host-obligation discharge.'],nineExpectedAcceptance=desc('/private/tmp/astra-paths-nine-expected-acceptance-20261010-a.json'),script=desc(__file__))
out.write_text(json.dumps(v,indent=2)+'\n');print(json.dumps(desc(out)))
