import hashlib,json
from pathlib import Path
from jsonschema import Draft202012Validator
from referencing import Registry
B=Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-f');E=B.with_name('ashlar-supply-chain-compile-candidate-20261010-e');P=Path('/private/tmp/astra-supplychain-corrected-envelope-review-20261010-f')
def desc(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def refuse(uri):raise AssertionError('Remote schema retrieval forbidden')
schema=Path('/private/tmp/ashlar-paths-keys-native10-preparation-20261010-a/installation/schemas/compile-request-v0.4.schema.json');v=Draft202012Validator(json.loads(schema.read_bytes()),registry=Registry(retrieve=refuse));rows=[]
for case in ['split-excursion','excursion','replay','lineage','sensor']:
 p=B/(case+'.request.json');r=json.loads(p.read_bytes());v.validate(r)
 old=(E/p.name).read_bytes();assert old.count(b'"interfaceVersion":"weft-backend/0.3.0",')==1
 assert p.read_bytes()==old.replace(b'"interfaceVersion":"weft-backend/0.3.0",',b'')
 r['target']['interfaceVersion']='weft-backend/0.3.0';errors=list(v.iter_errors(r));assert len(errors)==1 and list(errors[0].absolute_path)==['target'] and errors[0].validator=='additionalProperties'
 rows.append(desc(p))
review=Path('/private/tmp/astra-supplychain-wrapper-review-20261010-c.json');q=json.loads(review.read_bytes())
assert q['status']=='approved-source-only'
source=[x for x in q['source_opening_closing'] if x['path'].startswith('/Users/erik/Projects/ashlar/')]
for d in source:assert desc(Path(d['path']))==d
out={'status':'approved-corrected-source-only','resolved_finding':'SUPPLY-ENVELOPE-001','source_files':source,'request_schema':desc(schema),'positive_offline_requests':rows,'extra_target_interfaceVersion_negative_controls':5,'exact_raw_delta':'Only one 40-byte target.interfaceVersion member removed; all other original request bytes preserved.','source_behavior_review':desc(review),'counterexample':desc(Path('/private/tmp/astra-supplychain-envelope-counterexample-20261010-e.json')),'limits':['F preparation contains copied E outcomes and is explicitly unexecuted/unapproved for launch. A fresh successor command is a separate gate.','Schema validity and exact source correspondence do not assert actual compilation or native query acceptance.'],'review_script':desc(Path(__file__))}
P.with_suffix('.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'status':out['status'],'schema_controls':10,'receipt':str(P.with_suffix('.json'))}))
