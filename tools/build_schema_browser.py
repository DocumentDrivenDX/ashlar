"""Vendor a pinned UMF renderer and catalog exact Ashlar models; no schema rewrite."""
import argparse,hashlib,io,json,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
REVISION='90d03f40c5435001a895e315eb5515032a9c8d64'
PACKAGE='@documentdrivendx/umf-schema-browser'
VERSION='1.0.0'
PACKAGE_BYTES=845173
PACKAGE_SHA256='ae319400f48745b868c490948e2bf68cbd7a78e43f31f8d8a3b74d1de1d0f77d'
UPSTREAM='package/assets/'
MODEL=ROOT/'docs/helix/02-design/models/ashlar-delta-runtime'
TARGET=ROOT/'website/static/model/schema-browser'
p=argparse.ArgumentParser(description=__doc__);p.add_argument('umf_browser_package',type=Path);p.add_argument('--check',action='store_true');a=p.parse_args()
def sha(data):return hashlib.sha256(data).hexdigest()
with a.umf_browser_package.open('rb') as source:package=source.read(PACKAGE_BYTES+1)
if len(package)!=PACKAGE_BYTES or sha(package)!=PACKAGE_SHA256:raise ValueError('Untrusted UMF browser package')
with tarfile.open(fileobj=io.BytesIO(package),mode='r:gz') as archive:
 members=archive.getmembers()
 if len(members)!=14 or len({m.name for m in members})!=14 or any(not m.isfile() or m.name.startswith('/') or '..' in Path(m.name).parts or m.size>16*1024*1024 for m in members) or sum(m.size for m in members)>32*1024*1024:raise ValueError('Unexpected UMF browser package closure')
 def original(name):
  member=archive.getmember('package/'+name)
  with archive.extractfile(member) as source:return source.read(member.size+1)
 metadata=json.loads(original('package.json'))
 if metadata['name']!=PACKAGE or metadata['version']!=VERSION:raise ValueError('Unexpected UMF browser version')
 assets={name:original('assets/'+name) for name in ['explorer.js','explorer.css']}
 assets.update({name:original(name) for name in ['LICENSE-MIT','LICENSE-APACHE','THIRD_PARTY_NOTICES.md']})
entries=[];inputs={}
index=json.loads((MODEL/'index.json').read_text())
for name in ['relationships',*index['tables']]:
 file=MODEL/(name+'.umf.json');data=file.read_bytes();inputs[str(file.relative_to(ROOT))]=sha(data)
 entries.append({'id':'ashlar-runtime-logical' if name=='relationships' else 'ashlar-delta:'+name,'title':'Runtime records and relationships' if name=='relationships' else name+' · Delta definition','category':'domain','path':name+'.umf.json','text':data.decode(),'format':'json','description':'Authored logical fields, keys and endpoint relationships; storage enforcement is separate.' if name=='relationships' else 'Original Delta schema and layout retained in extension payloads.'})
inputs[str((MODEL/'index.json').relative_to(ROOT))]=sha((MODEL/'index.json').read_bytes())
assets['schema-catalog.json']=(json.dumps({'version':1,'entries':entries},indent=2)+'\n').encode()
for name in ['index.html','host.css']:
 assets[name]=(ROOT/'website/schema-browser'/name).read_bytes()
provenance={'profile':'ashlar-umf-schema-browser/0.1','upstream':{'project':'UMF','revision':REVISION,'path':UPSTREAM,'package':{'name':PACKAGE,'version':VERSION,'bytes':PACKAGE_BYTES,'sha256':PACKAGE_SHA256},'assets':{name:sha(assets[name]) for name in ['explorer.js','explorer.css']}},'inputs':inputs,'generator_sha256':sha(Path(__file__).read_bytes()),'outputs':{name:sha(data) for name,data in assets.items()},'qualification':'Exact published UMF standalone renderer, stylesheet and license notices. Nine exact original Ashlar UMF sources, including eight physical definitions and one logical model. Browser interpretation is explicitly partial for Delta extensions; no native validation or enforcement claim. Generated/shared UI is outside signed Markdown source coverage.'}
assets['provenance.json']=(json.dumps(provenance,indent=2)+'\n').encode()
for name,data in assets.items():
 path=TARGET/name
 if a.check:
  if not path.exists() or path.read_bytes()!=data:raise ValueError('Stale browser artifact: '+name)
 else:path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
print(('Verified' if a.check else 'Built')+' pinned UMF browser with nine exact Ashlar model sources')
