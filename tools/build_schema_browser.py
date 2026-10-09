"""Vendor a pinned UMF renderer and catalog exact Ashlar models; no schema rewrite."""
import argparse,hashlib,json,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
REVISION='c433cfcdde21995803aad65234f20ba95d8c3222'
UPSTREAM='docs/helix/05-deploy/microsite/dist/'
MODEL=ROOT/'docs/helix/02-design/models/ashlar-delta-runtime'
TARGET=ROOT/'website/static/model/schema-browser'
p=argparse.ArgumentParser(description=__doc__);p.add_argument('umf_source',type=Path);p.add_argument('--check',action='store_true');a=p.parse_args()
def sha(data):return hashlib.sha256(data).hexdigest()
def original(name):return subprocess.check_output(['git','-C',str(a.umf_source),'show',REVISION+':'+UPSTREAM+name])
assets={name:original(name) for name in ['explorer.js','explorer.css']}
entries=[];inputs={}
index=json.loads((MODEL/'index.json').read_text())
for name in ['relationships',*index['tables']]:
 file=MODEL/(name+'.umf.json');data=file.read_bytes();inputs[str(file.relative_to(ROOT))]=sha(data)
 entries.append({'id':'ashlar-runtime-logical' if name=='relationships' else 'ashlar-delta:'+name,'title':'Runtime records and relationships' if name=='relationships' else name+' · Delta definition','category':'domain','path':name+'.umf.json','text':data.decode(),'format':'json','description':'Authored logical fields, keys and endpoint relationships; storage enforcement is separate.' if name=='relationships' else 'Original Delta schema and layout retained in extension payloads.'})
inputs[str((MODEL/'index.json').relative_to(ROOT))]=sha((MODEL/'index.json').read_bytes())
assets['schema-catalog.json']=(json.dumps({'version':1,'entries':entries},indent=2)+'\n').encode()
for name in ['index.html','host.css']:
 assets[name]=(ROOT/'website/schema-browser'/name).read_bytes()
provenance={'profile':'ashlar-umf-schema-browser/0.1','upstream':{'project':'UMF','revision':REVISION,'path':UPSTREAM,'assets':{name:sha(assets[name]) for name in ['explorer.js','explorer.css']}},'inputs':inputs,'generator_sha256':sha(Path(__file__).read_bytes()),'outputs':{name:sha(data) for name,data in assets.items()},'qualification':'Pinned upstream browser bundle and stylesheet unchanged. Nine exact original Ashlar UMF sources, including eight physical definitions and one logical model. Browser interpretation is explicitly partial for Delta extensions; no native validation or enforcement claim. Generated/shared UI is outside signed Markdown source coverage.'}
assets['provenance.json']=(json.dumps(provenance,indent=2)+'\n').encode()
for name,data in assets.items():
 path=TARGET/name
 if a.check:
  if not path.exists() or path.read_bytes()!=data:raise ValueError('Stale browser artifact: '+name)
 else:path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
print(('Verified' if a.check else 'Built')+' pinned UMF browser with nine exact Ashlar model sources')
