"""CI fingerprint guard for the original Ashlar catalog and pinned UMF renderer."""
import hashlib,json
from pathlib import Path
root=Path(__file__).resolve().parents[2];folder=root/'website/static/model/schema-browser'
proof=json.loads((folder/'provenance.json').read_text())
sha=lambda data:hashlib.sha256(data).hexdigest()
assert proof['profile']=='ashlar-umf-schema-browser/0.1'
assert proof['upstream']['revision']=='00755f5d2b76696b14a4750214c66f1209560939'
assert proof['upstream']['package']=={'name':'@documentdrivendx/umf-schema-browser','version':'1.1.2','bytes':2290558,'sha256':'45d559a5d0d4ff22826fb7f00211ea8c320823a6f3aaf403df744c4d0086cd34'}
assert proof['generator_sha256']==sha((root/'tools/build_schema_browser.py').read_bytes())
release=folder/'upstream/release-manifest.json'
assert sha(release.read_bytes())=='6d78c7252c6888706628b7d9252d05968d6409d6e99d1fbc8a5eb5fd4476295a'
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
sources=[str((model/(name+'.umf.json')).relative_to(root))for name in ['relationships',*index['tables']]]+['examples/truss-native/cohort.umf.json','examples/truss-native/cohort-r2.umf.json','examples/truss-native/cohort-inverse.umf.json','examples/truss-native/typed-record.umf.json','examples/truss-native/typed-record-r2.umf.json','examples/truss-native/core08-asset.umf.json']
assert catalog['version']==1 and len(catalog['entries'])==len(sources)
assert proof['source_count']==len(sources)
assert set(proof['inputs'])==set(sources)|{str((model/'index.json').relative_to(root))}
assert [entry['source_repo_path']for entry in catalog['entries']]==sources
assert [entry['id']for entry in catalog['entries'][-6:]]==['truss-cohort-r1','truss-cohort-r2','truss-cohort-inverse','truss-typed-record-r1','truss-typed-record-r2','truss-core08-asset']
for entry in catalog['entries']:assert entry['text'].encode()==(root/entry['source_repo_path']).read_bytes()
print('Pinned UMF renderer and all '+str(len(sources))+' original schema sources match')
