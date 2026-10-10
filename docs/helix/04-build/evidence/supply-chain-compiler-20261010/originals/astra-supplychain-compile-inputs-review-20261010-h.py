import hashlib,json
from pathlib import Path
from jsonschema import Draft202012Validator
from referencing import Registry
B=Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-h');G=B.with_name('ashlar-supply-chain-compile-candidate-20261010-g');P=Path('/private/tmp/astra-supplychain-compile-inputs-review-20261010-h')
def desc(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
m=json.loads((B/'manifest.json').read_bytes());assert desc(B/'manifest.json')['sha256']=='e8a781257419d3b146934295e1eacbfdb6efdb320be8cd3ed1c28119a48bfe9c'
for d in m['files']:assert desc(Path(d['path']))==d
c=json.loads((B/'command.json').read_bytes());assert desc(B/'command.json')['sha256']=='649789f68102d390a6136c313047d468151e196b74c186eb4adcb5197171ebbf'
allowed={Path(d['path']).name for d in m['files']}|{'manifest.json'};actual={x.name for x in B.iterdir()}
assert actual==allowed or actual==allowed|{'opening-root.json'}
if (B/'opening-root.json').exists():
 op=json.loads((B/'opening-root.json').read_bytes());assert op['files']==c['resources'] and op['commandSha256']==desc(B/'command.json')['sha256']
assert (B/'capture.py').read_bytes()==(G/'capture.py').read_bytes()
assert len(c['resources'])==len({d['path'] for d in c['resources']})==125
for d in c['resources']:assert desc(Path(d['path']))==d
expected=json.loads((G/'command.json').read_text().replace(str(G),str(B)))
for i,d in enumerate(expected['resources']):
 if d['path'].startswith(str(B)):expected['resources'][i]=desc(Path(d['path']))
assert c==expected
assert c['environment']=={'PATH':'/usr/bin:/bin'} and c['argv'][1:]==['-I','-B',str(B/'capture.py')]
assert str(Path(c['argv'][0]).resolve())==c['interpreter_resolution']['resolved_path']
assert not Path(c['output']).exists()
for name in ('process.json','process.stdout.log','process.stderr.log','closing-root.json','opening-inputs.json'):assert not (B/name).exists()
schema=Path('/private/tmp/ashlar-paths-keys-native10-preparation-20261010-a/installation/schemas/compile-request-v0.4.schema.json')
def deny(uri):raise AssertionError('No external schema retrieval')
v=Draft202012Validator(json.loads(schema.read_bytes()),registry=Registry(retrieve=deny))
for case in c['cases']:v.validate(json.loads((B/(case+'.request.json')).read_bytes()))
out={'status':'inputs-and-outer-schema-pass','manifest':desc(B/'manifest.json'),'command':desc(B/'command.json'),'capture':desc(B/'capture.py'),'source_review':desc(Path('/private/tmp/astra-supplychain-binding-source-review-20261010-h.json')),'selected_resource_count':125,'outer_schema':desc(schema),'outer_schema_positive_count':5,'outputs_absent':True,'all_current_hashes_match':True,'capture_equal_prior_tested_loop':True,'limits':['This receipt covers current125-resource custody and outer request schema only; nested binding audit is independently recorded.','Compiler parse/capability admission remains unexecuted for H; original replay SQL unchanged.','No native/publication/source ACK qualification or full OS closure.'],'review_script':desc(Path(__file__))}
P.with_suffix('.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'status':out['status'],'receipt':str(P.with_suffix('.json'))}))
