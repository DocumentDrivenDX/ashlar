"""CI fingerprint guard for the original Ashlar catalog and pinned UMF renderer."""
import hashlib,json
from pathlib import Path
root=Path(__file__).resolve().parents[2];folder=root/'website/static/model/schema-browser'
proof=json.loads((folder/'provenance.json').read_text())
sha=lambda data:hashlib.sha256(data).hexdigest()
assert proof['profile']=='ashlar-umf-schema-browser/0.1'
assert proof['upstream']['revision']=='f35f761891e76284489806b3ca881e5ba3bf5a7f'
assert proof['upstream']['package']=={'name':'@documentdrivendx/umf-schema-browser','version':'1.1.0','bytes':1861847,'sha256':'12b7a4831f181d78e2cf6ae608445f80bb7ce738dcd32eded7fe2b0838bc50be'}
assert proof['generator_sha256']==sha((root/'tools/build_schema_browser.py').read_bytes())
release=folder/'upstream/release-manifest.json'
assert sha(release.read_bytes())=='ca65e82f3ab7dcf71b72ec30e4c1fde822c6cd557f6678932e9491a9d6840243'
manifest=json.loads(release.read_text())['sha256']
expected={'upstream/'+name for name in manifest}|{'upstream/release-manifest.json','upstream/SHA256SUMS','index.html','host.css','host.js','schema-catalog.json'}
assert set(proof['outputs'])==expected
assert {str(p.relative_to(folder)) for p in folder.rglob('*') if p.is_file()}==expected|{'provenance.json'}
for name,digest in proof['outputs'].items():assert sha((folder/name).read_bytes())==digest,name
for name,digest in manifest.items():assert proof['outputs']['upstream/'+name]==digest
assert proof['upstream']['assets']=={name:digest for name,digest in proof['outputs'].items() if name.startswith('upstream/')}
for name,digest in proof['inputs'].items():assert sha((root/name).read_bytes())==digest,name
for name in ['index.html','host.css','host.js']:assert (folder/name).read_bytes()==(root/'website/schema-browser'/name).read_bytes(),name
model=root/'docs/helix/02-design/models/ashlar-delta-runtime'
index=json.loads((model/'index.json').read_text());catalog=json.loads((folder/'schema-catalog.json').read_text())
assert catalog['version']==1 and len(catalog['entries'])==9
assert set(proof['inputs'])=={str((model/(name+'.umf.json')).relative_to(root)) for name in ['relationships',*index['tables']]}|{str((model/'index.json').relative_to(root))}
assert [entry['path'] for entry in catalog['entries']]==[name+'.umf.json' for name in ['relationships',*index['tables']]]
for entry in catalog['entries']:assert entry['text']==(model/entry['path']).read_text()
print('Pinned UMF renderer and all nine original Ashlar schema sources match')
