import hashlib,json
from pathlib import Path
B=Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-g');E=B.with_name('ashlar-supply-chain-compile-candidate-20261010-e');F=B.with_name('ashlar-supply-chain-compile-candidate-20261010-f');P=Path('/private/tmp/astra-supplychain-compile-command-review-20261010-g')
def desc(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
m=json.loads((B/'manifest.json').read_bytes());assert desc(B/'manifest.json')['sha256']=='85405481fb65de8c4fb450d567c8d6be93ebf52a598a4ad883122d83d275b0fb'
for d in m['files']:assert desc(Path(d['path']))==d
c=json.loads((B/'command.json').read_bytes());e=json.loads((E/'command.json').read_bytes())
assert desc(B/'command.json')['sha256']=='2727b27e150175d0bf6c494cf2c46e86ccdcfec0658b7c141bebd342d4c3488d'
allowed={'manifest.json','command.json','capture.py','supply_chain_weft_request.py','test_supply_chain_weft_request.py','schema-controls.json'}|{x+'.request.json' for x in c['cases']}
assert {x.name for x in B.iterdir()}==allowed and len(allowed)==11 and all(x.is_file() for x in B.iterdir())
assert (B/'capture.py').read_bytes()==(E/'capture.py').read_bytes()
assert len(c['resources'])==len({d['path'] for d in c['resources']})==125
for d in c['resources']:assert desc(Path(d['path']))==d
# All non-input runtime/resource descriptors are unchanged from approved E.
ext=lambda q: sorted([x for x in q['resources'] if not x['path'].startswith(str(B)) and not x['path'].startswith(str(E))],key=lambda x:x['path'])
assert ext(c)==ext(e)
expected=json.loads((E/'command.json').read_text().replace(str(E),str(B)))
for i,d in enumerate(expected['resources']):
 if d['path'].startswith(str(B)):expected['resources'][i]=desc(Path(d['path']))
assert c==expected
assert c['environment']=={'PATH':'/usr/bin:/bin'} and c['argv'][1:]==['-I','-B',str(B/'capture.py')]
assert Path(c['argv'][0]).resolve()==Path(c['interpreter_resolution']['resolved_path'])
assert not Path(c['output']).exists()
for name in ('process.json','process.stdout.log','process.stderr.log','opening-root.json','closing-root.json','opening-inputs.json'):assert not (B/name).exists()
for case in c['cases']:assert (B/(case+'.request.json')).read_bytes()==(F/(case+'.request.json')).read_bytes()
for name in ('supply_chain_weft_request.py','test_supply_chain_weft_request.py','schema-controls.json'):assert (B/name).read_bytes()==(F/name).read_bytes()
for d in c['resources']:assert desc(Path(d['path']))==d
out={'status':'approved-corrected-command-for-root-execution','command':desc(B/'command.json'),'manifest':desc(B/'manifest.json'),'capture':desc(B/'capture.py'),'selected_resources':125,'all_current_inputs_unchanged':True,'fresh_directory_exactly_10_inputs_plus_manifest':True,'cases':c['cases'],'source_and_schema_review':desc(Path('/private/tmp/astra-supplychain-corrected-envelope-review-20261010-f.json')),'prior_control_gate':desc(Path('/private/tmp/astra-supplychain-compile-command-review-20261010-e.json')),'capture_control_gate':desc(Path('/private/tmp/astra-supplychain-capture-controls-20261010-a.receipt.json')),'findings':[],'execution_scope':['Exact direct foreground env-i PATH-only argv/cwd. No outer forced-kill wrapper.','Installed per-compiler30s deadline/16MiBstdout/4096stderr/two-second reap unchanged; no hard whole-run filesystem deadline.','Five candidate observations only; actual responses need independent schema/semantic review.','E actual invalid-envelope outcomes and F unexecuted stale preparation remain historical. No old opening/closing/process evidence is present in G.','No native engine, publication/ACK proof, current source authority or full OS closure claim.'],'root_preconditions':['Reverify all125 selected resources and interpreter resolution immediately before invocation.','Candidate output/process artifacts must be absent at execution start.','Retain raw terminal streams and closing125-resource vector for any observed outcome.'],'review_script':desc(Path(__file__))}
P.with_suffix('.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'status':out['status'],'receipt':str(P.with_suffix('.json'))}))
