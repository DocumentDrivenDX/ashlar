"""Build the schema-browser microsite page from the published UMF package and Ashlar's models.

Usage: npm pack @documentdrivendx/umf-schema-browser@1.1.2 && python tools/build_schema_browser.py documentdrivendx-umf-schema-browser-1.1.2.tgz
Writes website/static/model/schema-browser/ (gitignored). The package comes from npm; nothing is committed or hash-gated.
"""
import argparse,io,json,shutil,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
PACKAGE='@documentdrivendx/umf-schema-browser'
MODEL=ROOT/'docs/helix/02-design/models/ashlar-delta-runtime'
TARGET=ROOT/'website/static/model/schema-browser'
p=argparse.ArgumentParser(description=__doc__);p.add_argument('umf_browser_package',type=Path);a=p.parse_args()
assets={}
with tarfile.open(a.umf_browser_package,mode='r:gz') as archive:
 for member in archive.getmembers():
  if not member.isfile() or not member.name.startswith('package/') or '..' in Path(member.name).parts:continue
  assets['upstream/'+member.name[len('package/'):]]=archive.extractfile(member).read()
metadata=json.loads(assets['upstream/package.json'])
if metadata['name']!=PACKAGE:raise ValueError('Unexpected UMF browser package '+metadata['name'])
entries=[]
index=json.loads((MODEL/'index.json').read_text())
for name in ['relationships',*index['tables']]:
 file=MODEL/(name+'.umf.json');data=file.read_bytes()
 entries.append({'id':'ashlar-runtime-logical' if name=='relationships' else 'ashlar-delta:'+name,'title':'Runtime records and relationships' if name=='relationships' else name+' · Delta definition','category':'domain','source_repo_path':str(file.relative_to(ROOT)),'path':name+'.umf.json','text':data.decode(),'format':'json','description':'Authored logical fields, keys and endpoint relationships; storage enforcement is separate.' if name=='relationships' else 'Original Delta schema and layout retained in extension payloads.'})
for revision,filename in [('r1','cohort.umf.json'),('r2','cohort-r2.umf.json'),('inverse','cohort-inverse.umf.json')]:
 file=ROOT/'examples/truss-native'/filename;data=file.read_bytes();source_path=str(file.relative_to(ROOT))
 entries.append({'id':'truss-cohort-'+revision,'title':'Truss cohort · '+revision,'category':'domain','source_repo_path':source_path,'path':'cohort-'+revision+'.umf.json','text':data.decode(),'format':'json','description':'Two record types and an independent directed relationship in UMF 0.7.'+(' Adds optional B-tag.' if revision=='r2' else ' Declares authored inverse B-A.' if revision=='inverse' else '')})
for revision,filename in [('r1','typed-record.umf.json'),('r2','typed-record-r2.umf.json')]:
 file=ROOT/'examples/truss-native'/filename;data=file.read_bytes();source_path=str(file.relative_to(ROOT))
 entries.append({'id':'truss-typed-record-'+revision,'title':'Truss typed record · '+revision,'category':'domain','source_repo_path':source_path,'path':'typed-record-'+revision+'.umf.json','text':data.decode(),'format':'json','description':'Original UMF 0.7 STRING key, integer, decimal and Boolean declarations with optional fields.'+(' Adds optional verified Boolean without changing previous fields or keys.' if revision=='r2' else '')})
file=ROOT/'examples/truss-native/core08-asset.umf.json';data=file.read_bytes();source_path=str(file.relative_to(ROOT))
entries.append({'id':'truss-core08-asset','title':'Truss asset · original UMF 0.8','category':'domain','source_repo_path':source_path,'path':'core08-asset.umf.json','text':data.decode(),'format':'json','description':'Original UMF 0.8 integer, decimal and Boolean declarations. The consumer vocabulary remains uninterpreted; original extension content is retained.'})
for identity,filename,title,description in [
 ('configured-typed-scalar-r1','typed-model.umf.json','Configured typed scalar · original revision 1','Original core 0.8 scalar source declarations; unnamed fields remain unchanged.'),
 ('configured-typed-scalar-named-r2','typed-named-model.umf.json','Configured typed scalar · named revision 2','Separately authored core 0.8 named scalar declarations; not evidence of model migration.'),
 ('configured-typed-graph','typed-graph-model.umf.json','Configured typed graph · records and relationships','Original core 0.8 Person and Place records, authored keys and independent knows/located relationships; declarations do not establish native enforcement.')]:
 file=ROOT/'examples/end-to-end'/filename;data=file.read_bytes();source_path=str(file.relative_to(ROOT))
 entries.append({'id':identity,'title':title,'schemaFormat':'umf','category':'domain','source_repo_path':source_path,'path':filename,'text':data.decode(),'format':'json','description':description})
assets['schema-catalog.json']=(json.dumps({'version':1,'entries':entries},indent=2)+'\n').encode()
for name in ['index.html','host.css','host.js']:
 assets[name]=(ROOT/'website/schema-browser'/name).read_bytes()
if TARGET.exists():shutil.rmtree(TARGET)
for name,data in assets.items():
 path=TARGET/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
print(f'Built UMF schema browser {metadata["version"]} with {len(entries)} model sources into {TARGET.relative_to(ROOT)}')
