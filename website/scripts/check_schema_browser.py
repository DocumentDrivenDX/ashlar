"""CI fingerprint guard for the original Ashlar catalog and pinned UMF renderer."""
import hashlib,json
from pathlib import Path
root=Path(__file__).resolve().parents[2];folder=root/'website/static/model/schema-browser'
proof=json.loads((folder/'provenance.json').read_text())
sha=lambda data:hashlib.sha256(data).hexdigest()
assert proof['profile']=='ashlar-umf-schema-browser/0.1'
assert proof['upstream']['revision']=='7fc5726b8752d8f8de6eba21f9d1fbfc6ee7cc53'
assert proof['generator_sha256']==sha((root/'tools/build_schema_browser.py').read_bytes())
expected={'explorer.js','explorer.css','index.html','host.css','schema-catalog.json'}
assert set(proof['outputs'])==expected
for name,digest in proof['outputs'].items():assert sha((folder/name).read_bytes())==digest,name
for name in ['explorer.js','explorer.css']:assert proof['outputs'][name]==proof['upstream']['assets'][name]
for name,digest in proof['inputs'].items():assert sha((root/name).read_bytes())==digest,name
for name in ['index.html','host.css']:assert (folder/name).read_bytes()==(root/'website/schema-browser'/name).read_bytes(),name
model=root/'docs/helix/02-design/models/ashlar-delta-runtime'
index=json.loads((model/'index.json').read_text());catalog=json.loads((folder/'schema-catalog.json').read_text())
assert catalog['version']==1 and len(catalog['entries'])==9
assert set(proof['inputs'])=={str((model/(name+'.umf.json')).relative_to(root)) for name in ['relationships',*index['tables']]}|{str((model/'index.json').relative_to(root))}
assert [entry['path'] for entry in catalog['entries']]==[name+'.umf.json' for name in ['relationships',*index['tables']]]
for entry in catalog['entries']:assert entry['text']==(model/entry['path']).read_text()
print('Pinned UMF renderer and all nine original Ashlar schema sources match')
