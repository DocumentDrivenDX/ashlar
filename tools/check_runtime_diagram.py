"""Small real-UMF integration check, including unsupported/stale model refusals."""
import hashlib,json,shutil,subprocess,sys,tempfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
FOLDER=Path('docs/helix/02-design/models/ashlar-delta-runtime')
OUTPUTS=['website/static/model/runtime-er.json','website/static/model/runtime-er.svg','website/layouts/_partials/runtime-er.generated.html']
source=Path(sys.argv[1]).resolve()
bun=shutil.which('bun')
if not bun:raise ValueError('Bun required for actual pinned UMF integration')
def run(root):
    return subprocess.run([bun,str(root/'tools/generate_runtime_diagram.ts'),str(source),'--check'],capture_output=True,text=True)
first=run(ROOT)
if first.returncode:raise ValueError(first.stdout+first.stderr)
model=json.loads((ROOT/OUTPUTS[0]).read_text())
if len(model['tables'])!=8 or len(model['relationships'])!=2:raise ValueError('Incomplete diagram inventory')
for table in model['tables']:
    if len({field['name'] for field in table['columns']})!=len(table['columns']):raise ValueError('Duplicate diagram field')
cases=[]
for kind in ['unknown-logical-content','missing-member','wrong-field-family','unknown-native-metadata']:
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp)
        shutil.copytree(ROOT/FOLDER,root/FOLDER)
        for relative in ['tools/generate_runtime_diagram.ts',*OUTPUTS]:
            target=root/relative;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/relative,target)
        before={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in OUTPUTS}
        path=root/FOLDER/'relationships.umf.json';doc=json.loads(path.read_text())
        if kind=='unknown-logical-content':doc['future']={'exact':'18446744073709551615'}
        if kind=='missing-member':doc['modules'][0]['elements'][0]['members'].pop()
        if kind=='wrong-field-family':next(e for e in doc['modules'][0]['elements'] if e['id']=='object_current.type_id')['scalarType']='string'
        if kind=='unknown-native-metadata':
            path=root/FOLDER/'object_current.umf.json';doc=json.loads(path.read_text())
            metadata=doc['modules'][0]['elements'][0]['extensions']['umf.delta']['root']['members']['fields']['items'][0]['members']['metadata']['members']
            metadata['future']={'kind':'string','value':'18446744073709551615'}
        path.write_text(json.dumps(doc)+'\n');result=run(root)
        if not result.returncode:raise ValueError('Unsafe model was projected: '+kind)
        if before!={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in OUTPUTS}:raise ValueError('Refusal modified prior output')
        cases.append({'case':kind,'refused':True,'prior_outputs_preserved':True})
result={'state':'passed','profile':'ashlar-runtime-er/0.1','umf_revision':model['umfRevision'],'tables':len(model['tables']),'physical_fields':sum(len(t['columns']) for t in model['tables']),'relationships':len(model['relationships']),'controls':cases,'qualification':'Actual pinned UMF schema/core readers and DDL generator used; complete current diagram byte regeneration and four refusal controls. No native SQL or end-to-end toolkit completion claim.'}
(ROOT/FOLDER/'diagram-integration.json').write_text(json.dumps(result,indent=2)+'\n')
print('Diagram regeneration and four semantic refusal controls pass')
