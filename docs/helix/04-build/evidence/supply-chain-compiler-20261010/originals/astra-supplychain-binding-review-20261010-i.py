import json,hashlib
from pathlib import Path
root=Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-h');observations=[]
def keys(v,s):assert type(v)is dict and set(v)==set(s.split())
def text(v):assert type(v)is str and 0<len(v.encode())<=4096 and '\0'not in v
def signed(v):assert type(v)is str and str(int(v))==v and -(2**63)<=int(v)<2**63
for case in ('split-excursion','excursion','replay','lineage','sensor'):
 p=root/(case+'.request.json');raw=p.read_bytes();q=json.loads(raw);b=json.loads(q['target']['bindingJson']);keys(b,'profile layoutRevision layoutSha256 modelPins publication records')
 assert b['profile']=='ashlar-databricks-candidate/0.1.0'and b['layoutRevision']=='ashlar-delta/0.3'and b['layoutSha256']=='ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e'
 assert hashlib.sha256(q['target']['bindingJson'].encode()).hexdigest()==q['target']['bindingSha256']
 assert b['modelPins']==[m['pin']for m in q['modules']]
 assert len(q['modules'])==1
 module=q['modules'][0];doc=json.loads(module['documentJson']);pin=module['pin'];assert hashlib.sha256(module['documentJson'].encode()).hexdigest()==pin['sha256']==pin['revision'];assert pin['documentId']==doc['id'];assert pin['umfVersion']==doc['umf']
 elements={(m['id'],e['id']):e for m in doc['modules']for e in m['elements']}
 publication=b['publication'];keys(publication,'id manifestUuid tables');text(publication['id']);text(publication['manifestUuid']);assert 1<=len(publication['tables'])<=128
 names=set();uuids=set()
 for t in publication['tables']:
  keys(t,'name uuid version');assert type(t['name'])is list and len(t['name'])==3
  for name in t['name']:text(name);assert len(name)<=255
  assert tuple(t['name'])not in names;names.add(tuple(t['name']));text(t['uuid']);assert t['uuid']not in uuids;uuids.add(t['uuid']);assert type(t['version'])is int and 0<=t['version']<2**63
 assert 1<=len(b['records'])<=4096
 ids=set();property_count=0;optional=0
 for r in b['records']:
  keys(r,'logical table kind sourceSystem typeId schemaRevision properties');assert r['kind']in ('object','edge','nodeProjection','edgeProjection');assert type(r['table'])is int and 0<=r['table']<len(publication['tables']);text(r['sourceSystem']);text(r['schemaRevision']);signed(r['typeId']);assert len(r['properties'])<=4096
  identity=r['logical'];keys(identity,'documentId revision module element');assert identity['documentId']==pin['documentId']and identity['revision']==pin['revision'];k=(identity['module'],identity['element']);assert k not in ids;ids.add(k);authored=elements[k];assert authored['kind']=='record';members={(x['module'],x['element'])for x in authored['members']};seen=set()
  for prop in r['properties']:
   keys(prop,'logical home');i=prop['logical'];keys(i,'documentId revision module element');assert i['documentId']==identity['documentId']and i['revision']==identity['revision'];k=(i['module'],i['element']);assert k in members and k not in seen;seen.add(k);field=elements[k];assert field['kind']=='field'and field['extensions']=={};h=prop['home'];assert set(h)in ({'kind','propertyId'},{'kind','propertyId','encoding'});assert h['kind']=='props';signed(h['propertyId'])
   if 'encoding'in h:assert h['encoding']=='ashlar-weft-json-native-null/0.1-candidate'and field['nullability']=='absent-allowed'and field['scalarType']in ('string','integer','decimal','boolean');optional+=1
   property_count+=1
 observations.append({'case':case,'requestSha256':hashlib.sha256(raw).hexdigest(),'records':len(ids),'properties':property_count,'optionalNativeNullHomes':optional,'status':'structural-and-original-model-checks-passed'})
value={'scope':'Read-only Python assertions against actual five request values and selected Rust admission rules. Not Rust/compiler/native execution or proof of whole compiler admission. Replay grammar refusal remains separate.','observations':observations,'sourcePins':[{ 'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in (Path('/private/tmp/weft-paths-keys-3a2a79c-build-20261010-a/source/crates/weft-databricks/src/binding.rs'),Path('/private/tmp/weft-paths-keys-3a2a79c-build-20261010-a/source/crates/weft-databricks/src/paths.rs'))]}
Path('/private/tmp/astra-supplychain-binding-review-20261010-i.json').write_text(json.dumps(value,indent=2)+'\n');print(json.dumps(value))
