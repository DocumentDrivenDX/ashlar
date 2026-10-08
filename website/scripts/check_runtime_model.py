"""CI guard: refuse a site whose generated ER model has stale source fingerprints."""
import hashlib,json,re
from pathlib import Path
root=Path(__file__).resolve().parents[2]
folder=root/'docs/helix/02-design/models/ashlar-delta-runtime'
model=json.loads((root/'website/static/model/runtime-er.json').read_text())
index=json.loads((folder/'index.json').read_text())
if len(index['tables'])!=8 or any(not isinstance(name,str) or not re.fullmatch('[a-z_]+',name) for name in index['tables']):
    raise ValueError('Unexpected table identifiers')
expected={'index.json','relationships.umf.json'}|{name+'.umf.json' for name in index['tables']}
if set(model['inputs'])!=expected:raise ValueError('Unexpected runtime model source inventory')
for name,digest in model['inputs'].items():
    if hashlib.sha256((folder/name).read_bytes()).hexdigest()!=digest:raise ValueError('Stale generated runtime ER model: '+name)
if model['umfRevision']!=index['umfRevision'] or [table['name'] for table in model['tables']]!=index['tables']:
    raise ValueError('Diagram provenance or table inventory differs')
outputs={'website/static/model/runtime-er.svg','website/layouts/_partials/runtime-er.generated.html'}
if model['generator']!={'tool':'tools/generate_runtime_diagram.ts','sha256':hashlib.sha256((root/'tools/generate_runtime_diagram.ts').read_bytes()).hexdigest()}:
    raise ValueError('Diagram generator fingerprint differs')
if set(model['outputs'])!=outputs:raise ValueError('Unexpected generated output inventory')
for name,digest in model['outputs'].items():
    if hashlib.sha256((root/name).read_bytes()).hexdigest()!=digest:raise ValueError('Stale generated diagram output: '+name)
print('Generated runtime ER source fingerprints match')
