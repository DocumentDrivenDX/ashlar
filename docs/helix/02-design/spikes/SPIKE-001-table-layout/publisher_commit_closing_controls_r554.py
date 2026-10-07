"""Offline completed-publication binding proof and adversarial refusals."""
import copy,hashlib,json
from pathlib import Path
from publisher_commit_closing_r553 import expected_after
from publisher_custody_r462 import CustodyMismatch,verify
B=Path(__file__).resolve().parent
P=B/'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json'
a=json.loads(P.read_text());initial=a['metadata']['expected'];base=a['base'];tables=a['tables'];events=a['commit_events']
bound=expected_after(initial,base,tables,events)
assert initial==a['metadata']['expected']
assert next(x for x in bound if x['key']=='base-object_current')['head']['version']=='8'
assert tables['object_current']['version']==6
# Independently native-qualified closing results preserve every schema/profile/head.
N=B/'out/native/closing_metadata_native_r549/summary.json'
n=json.loads(N.read_text())
native={x['key'].removeprefix('concurrent-'):x for x in n['cohorts'][0]['accepted']}
assert set(native)=={x['key'] for x in bound}
for e in bound:
 actual=copy.deepcopy(native[e['key']]);actual['key']=e['key'];verify(actual,e)
 assert actual['head']['queryHistoryStatementId']==e['head']['queryHistoryStatementId']
controls=[]
def refuse(label,change):
 args=copy.deepcopy([initial,base,tables,events]);change(args)
 try:expected_after(*args)
 except CustodyMismatch:controls.append(label)
 else:raise AssertionError(label)
refuse('missing commit',lambda x:x[3].pop())
refuse('duplicate role',lambda x:x[3].__setitem__(0,copy.deepcopy(x[3][1])))
refuse('wrong statement binding',lambda x:x[3][0].__setitem__('statement_id','foreign-statement'))
refuse('unexpected interval',lambda x:x[2]['edge_current'].__setitem__('version',10))
refuse('changed UUID',lambda x:x[2]['edge_current'].__setitem__('id','foreign-uuid'))
refuse('changed node selection',lambda x:x[2]['object_current'].__setitem__('version',8))
refuse('initial head mismatch',lambda x:next(e for e in x[0] if e['key']=='base-edge_current')['head'].__setitem__('version','7'))
refuse('missing input custody',lambda x:x[0].pop())
result={'state':'Completed sixth publisher closing binding passes; eight adversarial refusals','source_sha256':{str(p.relative_to(B)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [P,N,B/'publisher_commit_closing_r553.py']},'expected':bound,'controls':controls,'qualification':'Offline binding of actual qualified sixth commit receipts, not a new publication clock or native execution of this wrapper. Physical node head8 retained while selected snapshot remains6. Closed commit intervals and production fencing remain caller obligations.'}
(B/'out/publisher-commit-closing-controls-r554.json').write_text(json.dumps(result,indent=2)+'\n')
