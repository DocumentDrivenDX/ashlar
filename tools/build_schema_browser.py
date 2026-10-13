"""Vendor a pinned UMF renderer and catalog exact Ashlar models; no schema rewrite."""
import argparse,hashlib,io,json,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
REVISION='00755f5d2b76696b14a4750214c66f1209560939'
PACKAGE='@documentdrivendx/umf-schema-browser'
VERSION='1.1.2'
PACKAGE_BYTES=2290558
PACKAGE_SHA256='45d559a5d0d4ff22826fb7f00211ea8c320823a6f3aaf403df744c4d0086cd34'
UPSTREAM='package/'
MODEL=ROOT/'docs/helix/02-design/models/ashlar-delta-runtime'
TARGET=ROOT/'website/static/model/schema-browser'
p=argparse.ArgumentParser(description=__doc__);p.add_argument('umf_browser_package',type=Path);p.add_argument('--check',action='store_true');a=p.parse_args()
def sha(data):return hashlib.sha256(data).hexdigest()
with a.umf_browser_package.open('rb') as source:package=source.read(PACKAGE_BYTES+1)
if len(package)!=PACKAGE_BYTES or sha(package)!=PACKAGE_SHA256:raise ValueError('Untrusted UMF browser package')
with tarfile.open(fileobj=io.BytesIO(package),mode='r:gz') as archive:
 members=archive.getmembers()
 if len(members)!=85 or len({m.name for m in members})!=85 or any(not m.isfile() or not m.name.startswith('package/') or '..' in Path(m.name).parts or m.size>16*1024*1024 for m in members) or sum(m.size for m in members)>32*1024*1024:raise ValueError('Unexpected UMF browser package closure')
 assets={}
 for member in members:
  with archive.extractfile(member) as source:data=source.read(member.size+1)
  if len(data)!=member.size:raise ValueError('Incomplete package member')
  assets['upstream/'+member.name[len('package/'):]]=data
 metadata=json.loads(assets['upstream/package.json'])
 if metadata['name']!=PACKAGE or metadata['version']!=VERSION:raise ValueError('Unexpected UMF browser version')
 manifest=json.loads(assets['upstream/release-manifest.json'])['sha256']
 if set(manifest)!=set(name[len('upstream/'):] for name in assets)-{'release-manifest.json','SHA256SUMS'}:raise ValueError('Incomplete public release manifest')
 for name,digest in manifest.items():
  if sha(assets['upstream/'+name])!=digest:raise ValueError('Changed public release member')
upstream_assets={name:sha(data) for name,data in assets.items()}
entries=[];inputs={}
index=json.loads((MODEL/'index.json').read_text())
for name in ['relationships',*index['tables']]:
 file=MODEL/(name+'.umf.json');data=file.read_bytes();inputs[str(file.relative_to(ROOT))]=sha(data)
 entries.append({'id':'ashlar-runtime-logical' if name=='relationships' else 'ashlar-delta:'+name,'title':'Runtime records and relationships' if name=='relationships' else name+' · Delta definition','category':'domain','source_repo_path':str(file.relative_to(ROOT)),'path':name+'.umf.json','text':data.decode(),'format':'json','description':'Authored logical fields, keys and endpoint relationships; storage enforcement is separate.' if name=='relationships' else 'Original Delta schema and layout retained in extension payloads.'})
for revision,filename in [('r1','cohort.umf.json'),('r2','cohort-r2.umf.json'),('inverse','cohort-inverse.umf.json')]:
 file=ROOT/'examples/truss-native'/filename;data=file.read_bytes();source_path=str(file.relative_to(ROOT));inputs[source_path]=sha(data)
 entries.append({'id':'truss-cohort-'+revision,'title':'Truss cohort · '+revision,'category':'domain','source_repo_path':source_path,'path':'cohort-'+revision+'.umf.json','text':data.decode(),'format':'json','description':'Two record types and an independent directed relationship in UMF 0.7.'+(' Adds optional B-tag.' if revision=='r2' else ' Declares authored inverse B-A.' if revision=='inverse' else '')})
for revision,filename in [('r1','typed-record.umf.json'),('r2','typed-record-r2.umf.json')]:
 file=ROOT/'examples/truss-native'/filename;data=file.read_bytes();source_path=str(file.relative_to(ROOT));inputs[source_path]=sha(data)
 entries.append({'id':'truss-typed-record-'+revision,'title':'Truss typed record · '+revision,'category':'domain','source_repo_path':source_path,'path':'typed-record-'+revision+'.umf.json','text':data.decode(),'format':'json','description':'Original UMF 0.7 STRING key, integer, decimal and Boolean declarations with optional fields.'+(' Adds optional verified Boolean without changing previous fields or keys.' if revision=='r2' else '')})
inputs[str((MODEL/'index.json').relative_to(ROOT))]=sha((MODEL/'index.json').read_bytes())
assets['schema-catalog.json']=(json.dumps({'version':1,'entries':entries},indent=2)+'\n').encode()
for name in ['index.html','host.css','host.js']:
 assets[name]=(ROOT/'website/schema-browser'/name).read_bytes()
provenance={'profile':'ashlar-umf-schema-browser/0.1','upstream':{'project':'UMF','revision':REVISION,'path':UPSTREAM,'package':{'name':PACKAGE,'version':VERSION,'bytes':PACKAGE_BYTES,'sha256':PACKAGE_SHA256},'assets':upstream_assets},'inputs':inputs,'generator_sha256':sha(Path(__file__).read_bytes()),'outputs':{name:sha(data) for name,data in assets.items()},'source_count':len(entries),'qualification':f'Exact published UMF standalone renderer, stylesheet and license notices. {len(entries)} exact original UMF sources, including {len(index["tables"])} physical definitions, one logical runtime model and two authored Truss cohort revisions and one authored inverse cohort, plus two exact typed record revisions. Browser interpretation is explicitly partial for Delta extensions; no native validation or enforcement claim. Generated/shared UI is outside signed Markdown source coverage.'}
assets['provenance.json']=(json.dumps(provenance,indent=2)+'\n').encode()
if a.check:
 actual={str(path.relative_to(TARGET)) for path in TARGET.rglob('*') if path.is_file()}
 if actual!=set(assets):raise ValueError('Changed browser output closure')
else:
 for path in TARGET.rglob('*'):
  if path.is_file() and str(path.relative_to(TARGET)) not in assets:path.unlink()
for name,data in assets.items():
 path=TARGET/name
 if a.check:
  if not path.exists() or path.read_bytes()!=data:raise ValueError('Stale browser artifact: '+name)
 else:path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
print(('Verified' if a.check else 'Built')+f' pinned UMF browser with {len(entries)} exact model sources')
