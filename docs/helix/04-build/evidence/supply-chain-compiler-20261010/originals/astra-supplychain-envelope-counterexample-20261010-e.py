import hashlib,json
from pathlib import Path
from jsonschema import Draft202012Validator
from referencing import Registry
P=Path('/private/tmp/astra-supplychain-envelope-counterexample-20261010-e');B=Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-e')
def desc(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def refuse_remote(uri):raise AssertionError('No external schema retrieval allowed')
schema=Path('/private/tmp/ashlar-paths-keys-native10-preparation-20261010-a/installation/schemas/compile-request-v0.4.schema.json');validator=Draft202012Validator(json.loads(schema.read_bytes()),registry=Registry(retrieve=refuse_remote))
command=json.loads((B/'command.json').read_bytes());opening=json.loads((B/'opening-root.json').read_bytes());closing=json.loads((B/'closing-root.json').read_bytes())
assert opening==closing and len(opening['files'])==125
for d in command['resources']:assert desc(Path(d['path']))==d
records=[]
for case in command['cases']:
 p=B/(case+'.request.json');r=json.loads(p.read_bytes());errors=list(validator.iter_errors(r));assert len(errors)==1
 e=errors[0];assert list(e.absolute_path)==['target'] and e.validator=='additionalProperties' and 'interfaceVersion' in e.message
 response=B/'candidate-output'/(case+'.response.json');a=json.loads(response.read_bytes());assert len(response.read_bytes())==221 and a['status']=='blocked' and a['interfaceVersion']=='weft-compile/0.4.0' and [x['code'] for x in a['diagnostics']]==['WFT-INPUT']
 revised=json.loads(p.read_bytes());del revised['target']['interfaceVersion'];validator.validate(revised)
 records.append({'case':case,'request':desc(p),'response':desc(response),'schema_error':{'path':list(e.absolute_path),'validator':e.validator,'message':e.message},'remove_only_target_interfaceVersion_validates':True})
out={'status':'confirmed-wrapper-envelope-defect','finding':'SUPPLY-ENVELOPE-001','scope':'Actual five retained compiler refusal responses; no compiler rerun. Offline retained request schema independently identifies the invalid target member.','source':desc(B/'supply_chain_weft_request.py'),'schema':desc(schema),'cases':records,'custody':[desc(B/n) for n in ['opening-root.json','closing-root.json','process.json','process.stdout.log','process.stderr.log']],'all_125_current_unchanged':True,'review_correction':'The earlier wrapper source review incorrectly accepted spreading all PATHS_KEYS_BACKEND keys. Backend manifest interfaceVersion is not an allowed compile-request target property. Its source-ready conclusion is superseded for those two wrapper files; preparer/shared-runner findings remain unaffected.','required_correction':'Select only backendId/backendVersion/targetProfile for target; preserve original SQL/model/binding bytes. Add exact target-key-set regression and validate all five revised request envelopes offline before execution.','limits':['WFT-INPUT is request-envelope refusal, not evidence of query capability refusal.','Removing the unsupported member passes schema only; no new compiler/native acceptance implied.'],'review_script':desc(Path(__file__))}
P.with_suffix('.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'finding':out['finding'],'cases':5,'custody':125,'receipt':str(P.with_suffix('.json'))}))
